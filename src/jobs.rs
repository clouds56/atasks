use crate::core::*;
use crate::scheduler::*;

use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BTreeSet, BTreeMap};
use slab::Slab;

enum JobToRun<Job> {
  Running,
  Todo(Job),
}
impl<Job> JobToRun<Job> {
  fn take(&mut self) -> Option<Job> {
    match std::mem::replace(self, JobToRun::Running) {
      JobToRun::Running => None,
      JobToRun::Todo(t) => Some(t),
    }
  }
  fn restore(&mut self, t: Job) {
    if let JobToRun::Todo(_) = self {
      panic!("restore to job that occupies");
    }
    *self = JobToRun::Todo(t)
  }
}

#[derive(Default, Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry(u64);
#[derive(Clone, Copy)]
pub enum Status {
  NotStarted, Running, Waiting, Paused, Finished,
}
type Key = usize;

struct JobInfo<O, Controller> {
  pub priority: (O, Entry),
  pub controller: Controller,
}

#[derive(Debug)]
struct Current<K> {
  pub current: Vec<K>,
  pub todo: VecDeque<K>, // Paused
  pub tocancel: BTreeSet<K>, // Waiting
}
impl<K: Ord + Copy + std::fmt::Debug> Current<K> {
  fn new() -> Self {
    Self { current: Vec::new(), todo: VecDeque::new(), tocancel: BTreeSet::new(), }
  }
  fn renew(&mut self, target: &[K]) {
    debug!("renew {:?}", self);
    self.todo.clear();
    self.tocancel.extend(std::mem::replace(&mut self.current, Vec::with_capacity(target.len())));
    for &k in target {
      if self.tocancel.remove(&k) {
        self.current.push(k);
      } else {
        self.todo.push_back(k);
      }
    }
    debug!("renew2 {:?}", self);
  }
  fn take_cancel(&mut self) -> Vec<K> {
    debug!("take_cancel {:?}", self);
    std::mem::replace(&mut self.tocancel, BTreeSet::new()).into_iter().collect()
  }
  fn pop_todo(&mut self) -> Option<K> {
    debug!("pop_todo {:?}", self);
    self.todo.pop_front()
  }
  fn push_current(&mut self, k: K) {
    self.current.push(k);
  }
}

pub struct State;

pub trait Callback<T: Job>: FnMut(Progress) {}
impl<T: Job, F: FnMut(Progress)> Callback<T> for F { }

pub struct PriorityJobsState<O, T: Job> {
  idx: Entry,
  key_map: HashMap<Entry, Key>,
  jobs: Slab<JobToRun<T>>,
  info: HashMap<Key, JobInfo<O, T::Controller>>,
  queue: BTreeSet<(O, Entry)>,
  status: BTreeMap<(O, Entry), Status>,
  current: Current<Entry>,
  finished: VecDeque<(Entry, T)>,
  capacity: usize,
  finished_capacity: Option<usize>,
  callback: Option<Box<dyn FnMut(Progress) + Send>>,
}

impl<O: Ord + Copy, T: Job> PriorityJobsState<O, T> {
  fn new(capacity: usize, finished_capacity: Option<usize>) -> Self {
    Self {
      idx: Default::default(),
      key_map: Default::default(),
      jobs: Slab::new(),
      info: HashMap::new(),
      queue: BTreeSet::new(),
      status: BTreeMap::new(),
      current: Current::new(),
      finished: VecDeque::new(),
      capacity, finished_capacity,
      callback: None,
    }
  }
  fn new_with_callback(capacity: usize, finished_capacity: Option<usize>, callback: impl Callback<T> + Send + 'static) -> Self {
    let callback: Box<dyn FnMut(Progress) + Send> = Box::new(callback);
    Self {
      callback: Some(callback),
      ..Self::new(capacity, finished_capacity)
    }
  }

  pub fn add_job_resume(&mut self, priority: O, job: T) -> Entry {
    let entry = self.add_job(priority, job);
    self.status.insert((priority, entry), Status::Waiting);
    self.resume_job(entry);
    entry
  }
  pub fn add_job(&mut self, priority: O, job: T) -> Entry {
    let controller = job.controller();
    let key = self.jobs.insert(JobToRun::Todo(job));
    let entry = Entry(self.idx.0); self.idx.0 += 1;
    let priority = (priority, entry);
    self.info.insert(key, JobInfo { priority, controller });
    self.status.insert(priority, Status::NotStarted);
    self.key_map.insert(entry, key);
    entry
  }
  pub fn remove_job(&mut self, entry: Entry) -> Option<Status> {
    let key = self.key_map.remove(&entry)?;
    let mut info = self.info.remove(&key).expect("info");
    let status = self.status.remove(&info.priority).expect("status");
    if !info.controller.is_finished() { info.controller.shutdown(); }
    self.queue.remove(&info.priority);
    self.jobs.remove(key);
    Some(status)
  }
  pub fn resume_job(&mut self, entry: Entry) -> Option<()> {
    let key = self.key_map.get(&entry)?;
    let info = self.info.get_mut(key).expect("info");
    self.queue.insert(info.priority);
    Some(())
  }
  pub fn pause_job(&mut self, entry: Entry) -> Option<()> {
    let key = self.key_map.get(&entry)?;
    let info = self.info.get_mut(key).expect("info");
    self.queue.remove(&info.priority);
    Some(())
  }
  pub fn set_priority(&mut self, entry: Entry, priority: O) -> Option<O> {
    let key = self.key_map.get(&entry)?;
    let info = self.info.get_mut(key).expect("info");
    let old_priority = info.priority.0;
    if priority == old_priority {
      return Some(priority)
    }
    let priority = (priority, info.priority.1);
    if self.queue.remove(&info.priority) {
      self.queue.insert(priority);
    }
    let status = self.status.remove(&info.priority).expect("status");
    self.status.insert(priority, status);
    info.priority = priority;
    Some(old_priority)
  }

  fn take_finished(&mut self) -> VecDeque<(Entry, T)> {
    std::mem::replace(&mut self.finished, VecDeque::new())
  }
}

impl<O: Ord + Copy, T: Job> Schedulable<Entry, T> for PriorityJobsState<O, T> {
  type Message = Progress;
  fn flush(&mut self) {
    while self.finished_capacity.map(|cap| self.finished.len() > cap) == Some(true) {
      self.finished.pop_front();
    }
    let new_current = self.queue.iter().take(self.capacity).map(|(_, entry)| *entry).collect::<Vec<_>>();
    self.current.renew(&new_current);
    for entry in self.current.take_cancel() {
      // maybe removed/finished
      if let Some(&key) = self.key_map.get(&entry) {
        let info = self.info.get_mut(&key).expect("info");
        info.controller.stop();
      }
    }
  }

  fn restore(&mut self, entry: Entry, job: T) {
    let key = self.key_map[&entry];
    let info = self.info.get(&key).expect("info");
    info!("restore: {:?} {}", entry, info.controller.is_finished());
    if !info.controller.is_finished() {
      self.jobs.get_mut(key).expect("jobs").restore(job);
    } else {
      self.remove_job(entry);
      self.finished.push_back((entry, job));
    }
    // TODO check complete here?
  }

  fn fetch(&mut self) -> Option<(Entry, T)> {
    while let Some(entry) = self.current.pop_todo() {
      if let Some(&key) = self.key_map.get(&entry) {
        if let Some(t) = self.jobs.get_mut(key).expect("jobs").take() {
          info!("fetch {:?}", entry);
          self.current.push_current(entry);
          return Some((entry, t))
        } // do not push_back, if haven't restored, it would finish sooner or later
      }
    }
    None
  }

  fn callback(&mut self, _entry: Entry, _idx: usize, p: Self::Message) {
    if let Some(cb) = self.callback.as_mut() {
      cb(p)
    }
  }
}

pub struct PriorityJobs<Priority: Ord + Copy, Job: crate::core::Job> {
  state: Arc<Mutex<PriorityJobsState<Priority, Job>>>,
  scheduler: Scheduler<Entry, Job, PriorityJobsState<Priority, Job>>,
}
impl<O: Ord + Copy, T: Job> PriorityJobs<O, T> {
  pub fn new(capacity: usize, finished_capacity: Option<usize>) -> Self {
    let state = Arc::new(Mutex::new(PriorityJobsState::new(capacity, finished_capacity)));
    Self {
      state: state.clone(),
      scheduler: Scheduler::new(capacity, state),
    }
  }
  pub fn new_with_callback(capacity: usize, finished_capacity: Option<usize>, callback: impl Callback<T> + Send + 'static) -> Self {
    let state = Arc::new(Mutex::new(PriorityJobsState::new_with_callback(capacity, finished_capacity, callback)));
    Self {
      state: state.clone(),
      scheduler: Scheduler::new(capacity, state),
    }
  }

  pub fn update<R, F: FnOnce(&mut PriorityJobsState<O, T>) -> R>(&self, f: F) -> R {
    let r = f(&mut self.state.lock().unwrap());
    self.scheduler.flush();
    r
  }

  pub fn finished(&self) -> Vec<(Entry, T)> {
    self.state.lock().unwrap().take_finished().into_iter().collect()
  }
}

impl<O: Ord + Copy, I, T: Job<Item=(usize, I)> + Unpin + Send + 'static> PriorityJobs<O, T>
  where PriorityJobsState<O, T>: Send + 'static {
  pub fn schedule(&mut self) {
    self.scheduler.new_workers();
    self.scheduler.spawn();
  }

  pub fn wait(&mut self) {
    self.scheduler.wait();
  }

  pub fn finalize(&mut self) {
    self.scheduler.join();
    self.scheduler.join_workers();
  }
}
