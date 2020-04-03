use crate::core::*;

use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap, BTreeSet, BTreeMap};
use slab::Slab;

enum TaskToRun<Task> {
  Running,
  Todo(Task),
}
impl<Task> TaskToRun<Task> {
  fn take(&mut self) -> Task {
    match std::mem::replace(self, TaskToRun::Running) {
      TaskToRun::Running => panic!("take task already running"),
      TaskToRun::Todo(t) => t,
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

struct Current<K> {
  pub current: Vec<K>,
  pub todo: VecDeque<K>, // Paused
  pub tocancel: BTreeSet<K>, // Waiting
}
impl<K: Ord + Copy> Current<K> {
  fn new() -> Self {
    Self { current: Vec::new(), todo: VecDeque::new(), tocancel: BTreeSet::new(), }
  }
  fn renew(&mut self, target: &[K]) {
    self.tocancel.extend(std::mem::replace(&mut self.current, Vec::with_capacity(target.len())));
    for &k in target {
      if self.tocancel.remove(&k) {
        self.current.push(k);
      } else {
        self.todo.push_back(k);
      }
    }
  }
  fn take_cancel(&mut self) -> Vec<K> {
    std::mem::replace(&mut self.tocancel, BTreeSet::new()).into_iter().collect()
  }
  fn pop_todo(&mut self) -> Option<K> {
    self.todo.pop_front()
  }
}

pub struct State;

pub trait Callback<T: Task>: FnMut(<<T as Task>::Controller as AsProgress>::Progress) {}
impl<T: Task, F: FnMut(<<T as Task>::Controller as AsProgress>::Progress)> Callback<T> for F { }

pub struct PriorityTasksState<O: Ord + Copy, T: Task> {
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
  callback: Option<Box<dyn Callback<T> + Send>>,
}

impl<O: Ord + Copy, T: Task> PriorityTasksState<O, T> {
  fn new(capacity: usize, finished_capacity: Option<usize>, callback: Option<impl Callback<T> + Send + 'static>) -> Self {
    let callback = callback.map(|f| -> Box<dyn Callback<T> + Send> { Box::new(f) });
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
      callback,
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
    info.controller.shutdown();
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
}

pub struct PriorityTasks<Priority: Ord + Copy, Task: crate::core::Task> {
  state: Arc<Mutex<PriorityTasksState<Priority, Task>>>,
  tx: (),
  main: (),
  capacity: usize,
  worker: Vec<()>,
}
impl<O: Ord + Copy, T: Task> PriorityTasks<O, T> {
  pub fn new(capacity: usize, finished_capacity: Option<usize>, callback: Option<impl Callback<T> + Send + 'static>) -> Self {
    Self {
      state: Arc::new(Mutex::new(PriorityTasksState::new(capacity, finished_capacity, callback))),
      tx: (),
      main: (),
      capacity,
      worker: vec![],
    }
  }

  pub fn update<F: FnOnce(&mut PriorityTasksState<O, T>)>(&self, f: F) {
    f(&mut self.state.lock().unwrap())
  }
}
