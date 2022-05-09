use std::fmt::Debug;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::core::*;

pub(crate) use crossbeam_channel::{unbounded as channel, Sender, Receiver};

pub trait Schedulable<E, T> {
  type Message;
  fn flush(&mut self);
  fn restore(&mut self, entry: E, task: T);
  fn fetch(&mut self) -> Option<(E, T)>;
  fn callback(&mut self, _entry: E, _idx: usize, _p: Self::Message) {}
}

pub enum Message<E, P, T> {
  Progress(usize, E, usize, P),
  Finish(usize, E, T),
  NewWorker(usize),
  Flush,
  Wait,
  Close,
}

enum MaybeJoin<T> {
  Join(JoinHandle<T>),
  Some(T),
  Uninit,
}
impl<T> MaybeJoin<T> {
  fn update<F: FnOnce(T) -> Self>(&mut self, f: F) {
    if let MaybeJoin::Some(_) = self {
      if let MaybeJoin::Some(t) = std::mem::replace(self, MaybeJoin::Uninit) {
        *self = f(t)
      }
    }
  }
  fn join(&mut self) {
    if let MaybeJoin::Join(_) = self {
      if let MaybeJoin::Join(t) = std::mem::replace(self, MaybeJoin::Uninit) {
        *self =MaybeJoin::Some(t.join().expect("join failed"))
      }
    }
  }
  fn is_join(&self) -> bool { if let MaybeJoin::Join(_) = self { true } else { false } }
  fn is_some(&self) -> bool { if let MaybeJoin::Some(_) = self { true } else { false } }
}

pub struct Worker<E, P, T> {
  idx: usize,
  tx: Sender<Message<E, P, T>>,
  handler: MaybeJoin<Receiver<Option<(E, T)>>>,
}

impl<E, P, T> Worker<E, P, T> {
  pub fn new(idx: usize, rx: Receiver<Option<(E, T)>>, tx: Sender<Message<E, P, T>>) -> Self {
    Self { idx, tx, handler: MaybeJoin::Some(rx) }
  }
}

impl<E: Copy + Debug, I, P, T: Job<Item=(usize, I)> + Unpin> Worker<E, P, T>
  where P: From<Progress> {
  pub fn event_loop(idx: usize, rx: Receiver<Option<(E, T)>>, tx: Sender<Message<E, P, T>>) -> Receiver<Option<(E, T)>> {
    use futures::StreamExt;
    futures::executor::block_on(async {
      while let Ok(Some((entry, mut t))) = rx.recv() {
        let controller = t.controller();
        while let Some((i, _)) = t.next().await {
          trace!("worker {} progress {:?} {}", idx, entry, i);
          let _ = tx.send(Message::Progress(idx, entry, i, controller.progress().into()));
        }
        info!("worker {} finished {:?}", idx, entry);
        let _ = tx.send(Message::Finish(idx, entry, t));
      }
    });
    rx
  }
}

impl<E: Copy + Debug, I, P, T: Job<Item=(usize, I)> + Unpin> Worker<E, P, T>
  where E: Send + 'static, T: Send + 'static, P: From<Progress> + Send + 'static {
  fn spawn(&mut self) {
    let tx = self.tx.clone();
    let idx = self.idx;
    self.handler.update(move |rx| {
      MaybeJoin::Join(std::thread::spawn(move || { Self::event_loop(idx, rx, tx) }))
    })
  }
  fn join(&mut self) {
    self.handler.join();
  }
}


type Dispatcher<E, T> = HashMap<usize, (Sender<Option<(E, T)>>, bool)>;

pub struct Scheduler<E, T, S: Schedulable<E, T>> {
  tx: Sender<Message<E, S::Message, T>>,
  capacity: usize,
  dispatcher: Arc<Mutex<Dispatcher<E, T>>>,
  workers: Vec<Worker<E, S::Message, T>>,
  state: Arc<Mutex<S>>,
  handler: MaybeJoin<Receiver<Message<E, S::Message, T>>>,
}

impl<E, T, S: Schedulable<E, T>> Scheduler<E, T, S> {
  pub fn new(capacity: usize, state: Arc<Mutex<S>>) -> Self {
    let (tx, rx) = channel();
    Self {
      tx, state, capacity,
      dispatcher: Arc::new(Mutex::new(Dispatcher::new())),
      workers: Vec::new(), handler: MaybeJoin::Some(rx),
    }
  }

  pub fn flush(&self) {
    let _ = self.tx.send(Message::Flush);
  }
}

impl<E: Copy + Debug + Send + 'static, I, T: Job<Item=(usize, I)> + Unpin + Send + 'static, S: Schedulable<E, T>> Scheduler<E, T, S>
  where S::Message: From<Progress> + Send + 'static {
  pub fn new_worker(&mut self) {
    if self.workers.len() >= self.capacity { return }
    let (tx, rx) = channel();
    let idx = self.workers.len();
    let mut worker = Worker::new(idx, rx, self.tx.clone());
    self.dispatcher.lock().unwrap().insert(idx, (tx, true));
    worker.spawn();
    self.workers.push(worker);
    let _ = self.tx.send(Message::NewWorker(idx));
  }

  pub fn new_workers(&mut self) {
    (self.workers.len()..self.capacity).for_each(|_| self.new_worker());
  }
  pub fn join_workers(&mut self) {
    self.workers.iter_mut().for_each(|w| w.join());
  }
}

impl<E: Copy, I, T: Job<Item=(usize, I)> + Unpin, S: Schedulable<E, T>> Scheduler<E, T, S> {
  fn event_loop(rx: Receiver<Message<E, S::Message, T>>, dispatcher: Arc<Mutex<Dispatcher<E, T>>>, state: Arc<Mutex<S>>) -> Receiver<Message<E, S::Message, T>> {
    let mut retry = None;
    let mut wait = false;
    while let Ok(m) = rx.recv() {
      match m {
        Message::Finish(i, entry, task) => {
          let mut state = state.lock().unwrap();
          state.restore(entry, task);
          if let Some((entry, task)) = retry.or_else(|| state.fetch()) {
            retry = dispatcher.lock().unwrap().get(&i).unwrap().0.send(Some((entry, task))).err().map(|e| e.0.unwrap());
          } else {
            retry = None;
            dispatcher.lock().unwrap().get_mut(&i).unwrap().1 = true;
            state.flush();
          }
        },
        Message::Progress(_, entry, idx, p) => {
          let mut state = state.lock().unwrap();
          state.callback(entry, idx, p);
          continue;
        },
        Message::NewWorker(_) => continue,
        Message::Wait => { wait = true },
        Message::Close => {
          dispatcher.lock().unwrap().values().for_each(|(v, _)| {
            let _ = v.send(None);
          });
          break
        },
        Message::Flush => {
          debug!("flush");
          let mut state = state.lock().unwrap();
          state.flush();
        }
      }
      let mut busy = 0;
      for (_, v) in dispatcher.lock().unwrap().iter_mut() {
        if v.1 {
          let mut state = state.lock().unwrap();
          if let Some((entry, task)) = retry.or_else(|| state.fetch()) {
            retry = v.0.send(Some((entry, task))).err().map(|e| e.0.unwrap());
            v.1 = false;
          } else {
            retry = None;
          }
        } else {
          busy += 1;
        }
      }
      debug!("{} workers busy", busy);
      if wait && busy == 0 {
        break;
      }
    }
    rx
  }

  pub fn run(&mut self) {
    if !self.handler.is_some() { return }
    let state = self.state.clone();
    let dispatcher = self.dispatcher.clone();
    self.handler.update(|rx| MaybeJoin::Some(Self::event_loop(rx, dispatcher, state)));
  }
}

impl<E: Copy, I, T: Job<Item=(usize, I)> + Unpin, S: Schedulable<E, T>> Scheduler<E, T, S>
  where E: Send + 'static, T: Send + 'static, S: Send + 'static, S::Message: Send + 'static {
  pub fn spawn(&mut self) {
    if !self.handler.is_some() { return }
    let state = self.state.clone();
    let dispatcher = self.dispatcher.clone();
    self.handler.update(|rx| {
      MaybeJoin::Join(std::thread::spawn(|| { Self::event_loop(rx, dispatcher, state) }))
    });
  }

  pub fn wait(&mut self) {
    if !self.handler.is_join() { return }
    let _ = self.tx.send(Message::Wait);
    self.handler.join()
  }

  pub fn join(&mut self) {
    if !self.handler.is_join() { return }
    let _ = self.tx.send(Message::Close);
    self.handler.join()
  }
}

#[cfg(test)]
mod test {
  use crate::queue::TaskQueue;
  use crate::test::*;
  use super::*;
  struct Sc(usize);
  impl Schedulable<usize, T> for Sc {
    type Message = Progress;
    fn flush(&mut self) { }
    fn restore(&mut self, _entry: E, _task: T) {
      // println!("restore {}", entry);
    }
    fn fetch(&mut self) -> Option<(E, T)> {
      self.0 += 1;
      // println!("fetch {}", self.0);
      Some((self.0, TaskQueue::from_queue(QueueData(10), 3)))
    }
    fn callback(&mut self, _entry: E, _idx: usize, _p: Self::Message) {}
  }
  type E = usize;
  type T = TaskQueue<QueueData>;
  type S = Scheduler<usize, T, Sc>;
  #[test]
  fn test_scheduler() {
    let mut s = S::new(3, Arc::new(Mutex::new(Sc(0))));
    s.new_workers();
    s.spawn();
    assert_eq!(s.workers.len(), 3);
    let _ = s.tx.send(Message::Flush);
    std::thread::sleep(std::time::Duration::from_millis(200));
    println!("scheduled {} tasks", s.state.lock().unwrap().0);
    s.join();
    s.join_workers();
  }
}
