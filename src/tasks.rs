use crate::core::*;
use crate::scheduler::*;

use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BTreeSet, BTreeMap};
use slab::Slab;

enum TaskToRun<Task> {
  Running,
  Todo(Task),
}
impl<Task> TaskToRun<Task> {
  fn take(&mut self) -> Option<Task> {
    match std::mem::replace(self, TaskToRun::Running) {
      TaskToRun::Running => None,
      TaskToRun::Todo(t) => Some(t),
    }
  }
  fn restore(&mut self, t: Task) {
    if let TaskToRun::Todo(_) = self {
      panic!("restore to task that occupies");
    }
    std::mem::replace(self, TaskToRun::Todo(t));
  }
}

#[derive(Default, Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry(u64);
#[derive(Clone, Copy)]
pub enum Status {
  NotStarted, Running, Waiting, Paused, Finished,
}
type Key = usize;

struct TaskInfo<O, Controller> {
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

// TODO: ICE https://github.com/rust-lang/rust/issues/70746
pub trait Callback<T: Task>: FnMut(<<T as Task>::Controller as AsProgress>::Progress) {}
impl<T: Task, F: FnMut(<<T as Task>::Controller as AsProgress>::Progress)> Callback<T> for F { }

pub struct PriorityTasksState<O, T: Task> {
  idx: Entry,
  key_map: HashMap<Entry, Key>,
  tasks: Slab<TaskToRun<T>>,
  info: HashMap<Key, TaskInfo<O, T::Controller>>,
  queue: BTreeSet<(O, Entry)>,
  status: BTreeMap<(O, Entry), Status>,
  current: Current<Entry>,
  finished: VecDeque<(Entry, T)>,
  capacity: usize,
  finished_capacity: Option<usize>,
  #[allow(clippy::type_complexity)]
  callback: Option<Box<dyn FnMut(<<T as Task>::Controller as AsProgress>::Progress) + Send>>,
}

impl<O: Ord + Copy, T: Task> PriorityTasksState<O, T> {
  fn new(capacity: usize, finished_capacity: Option<usize>) -> Self {
    Self {
      idx: Default::default(),
      key_map: Default::default(),
      tasks: Slab::new(),
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
    let callback: Box<dyn FnMut(<<T as Task>::Controller as AsProgress>::Progress) + Send> = Box::new(callback);
    Self {
      callback: Some(callback),
      ..Self::new(capacity, finished_capacity)
    }
  }

  pub fn add_task_resume(&mut self, priority: O, task: T) -> Entry {
    let entry = self.add_task(priority, task);
    self.status.insert((priority, entry), Status::Waiting);
    self.resume_task(entry);
    entry
  }
  pub fn add_task(&mut self, priority: O, task: T) -> Entry {
    let controller = task.controller();
    let key = self.tasks.insert(TaskToRun::Todo(task));
    let entry = Entry(self.idx.0); self.idx.0 += 1;
    let priority = (priority, entry);
    self.info.insert(key, TaskInfo { priority, controller });
    self.status.insert(priority, Status::NotStarted);
    self.key_map.insert(entry, key);
    entry
  }
  pub fn remove_task(&mut self, entry: Entry) -> Option<Status> {
    let key = self.key_map.remove(&entry)?;
    let mut info = self.info.remove(&key).expect("info");
    let status = self.status.remove(&info.priority).expect("status");
    if !info.controller.is_finished() { info.controller.shutdown(); }
    self.queue.remove(&info.priority);
    self.tasks.remove(key);
    Some(status)
  }
  pub fn resume_task(&mut self, entry: Entry) -> Option<()> {
    let key = self.key_map.get(&entry)?;
    let info = self.info.get_mut(key).expect("info");
    self.queue.insert(info.priority);
    Some(())
  }
  pub fn pause_task(&mut self, entry: Entry) -> Option<()> {
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

impl<O: Ord + Copy, T: Task> Schedulable<Entry, T> for PriorityTasksState<O, T> {
  type Message = <T::Controller as AsProgress>::Progress;
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

  fn restore(&mut self, entry: Entry, task: T) {
    let key = self.key_map[&entry];
    let info = self.info.get(&key).expect("info");
    info!("restore: {:?} {}", entry, info.controller.is_finished());
    if !info.controller.is_finished() {
      self.tasks.get_mut(key).expect("tasks").restore(task);
    } else {
      self.remove_task(entry);
      self.finished.push_back((entry, task));
    }
    // TODO check complete here?
  }

  fn fetch(&mut self) -> Option<(Entry, T)> {
    while let Some(entry) = self.current.pop_todo() {
      if let Some(&key) = self.key_map.get(&entry) {
        if let Some(t) = self.tasks.get_mut(key).expect("tasks").take() {
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

pub struct PriorityTasks<Priority: Ord + Copy, Task: crate::core::Task> {
  state: Arc<Mutex<PriorityTasksState<Priority, Task>>>,
  scheduler: Scheduler<Entry, Task, PriorityTasksState<Priority, Task>>,
}
impl<O: Ord + Copy, T: Task> PriorityTasks<O, T> {
  pub fn new(capacity: usize, finished_capacity: Option<usize>) -> Self {
    let state = Arc::new(Mutex::new(PriorityTasksState::new(capacity, finished_capacity)));
    Self {
      state: state.clone(),
      scheduler: Scheduler::new(capacity, state),
    }
  }
  pub fn new_with_callback(capacity: usize, finished_capacity: Option<usize>, callback: impl Callback<T> + Send + 'static) -> Self {
    let state = Arc::new(Mutex::new(PriorityTasksState::new_with_callback(capacity, finished_capacity, callback)));
    Self {
      state: state.clone(),
      scheduler: Scheduler::new(capacity, state),
    }
  }

  pub fn update<R, F: FnOnce(&mut PriorityTasksState<O, T>) -> R>(&self, f: F) -> R {
    let r = f(&mut self.state.lock().unwrap());
    self.scheduler.flush();
    r
  }

  pub fn finished(&self) -> Vec<(Entry, T)> {
    self.state.lock().unwrap().take_finished().into_iter().collect()
  }
}

impl<O: Ord + Copy, I, T: Task<Item=(usize, I)> + Unpin + Send + 'static> PriorityTasks<O, T>
  where <T::Controller as AsProgress>::Progress: Send + 'static, PriorityTasksState<O, T>: Send + 'static {
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
