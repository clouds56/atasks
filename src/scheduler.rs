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
}

pub struct Worker<E, P, T> {
  idx: usize,
  rx: Option<Receiver<(E, T)>>,
  tx: Sender<Message<E, P, T>>,
}

impl<E, P, T> Worker<E, P, T> {
  pub fn new(idx: usize, rx: Receiver<(E, T)>, tx: Sender<Message<E, P, T>>) -> Self {
    Self { idx, rx: Some(rx), tx }
  }
}

impl<E: Copy, I, P, T: Task<Item=(usize, I)> + Unpin> Worker<E, P, T>
  where P: From<<T::Controller as AsProgress>::Progress> {
  pub fn event_loop(idx: usize, rx: Receiver<(E, T)>, tx: Sender<Message<E, P, T>>) -> Receiver<(E, T)> {
    use futures::StreamExt;
    futures::executor::block_on(async {
      while let Ok((entry, mut t)) = rx.recv() {
        let controller = t.controller();
        while let Some((i, _)) = t.next().await {
          let _ = tx.send(Message::Progress(idx, entry, i, controller.progress().into()));
        }
        let _ = tx.send(Message::Finish(idx, entry, t));
      }
    });
    rx
  }
}

impl<E: Copy + Send + 'static, I, P, T: Task<Item=(usize, I)> + Unpin + Send + 'static> Worker<E, P, T>
  where P: From<<T::Controller as AsProgress>::Progress> + Send + 'static {
  fn spawn(&mut self) {
    let rx = self.rx.take().expect("worker already spawn");
    let tx = self.tx.clone();
    let idx = self.idx;
    std::thread::spawn(move || { Self::event_loop(idx, rx, tx); });
  }
}


type Dispatcher<E, T> = HashMap<usize, (Sender<(E, T)>, bool)>;

pub struct Scheduler<E, T, S: Schedulable<E, T>> {
  mrx: Option<Receiver<Message<E, S::Message, T>>>,
  mtx: Sender<Message<E, S::Message, T>>,
  capacity: usize,
  dispatcher: Arc<Mutex<Dispatcher<E, T>>>,
  workers: Vec<Worker<E, S::Message, T>>,
  state: Arc<Mutex<S>>,
  handler: Option<JoinHandle<()>>,
}

impl<E, T, S: Schedulable<E, T>> Scheduler<E, T, S> {
  pub fn new(capacity: usize, state: Arc<Mutex<S>>) -> Self {
    let (mtx, mrx) = channel();
    Self {
      mtx, mrx: Some(mrx), state, capacity,
      dispatcher: Arc::new(Mutex::new(Dispatcher::new())),
      workers: Vec::new(), handler: None,
    }
  }
}

impl<E: Copy + Send + 'static, I, T: Task<Item=(usize, I)> + Unpin + Send + 'static, S: Schedulable<E, T>> Scheduler<E, T, S>
  where S::Message: From<<T::Controller as AsProgress>::Progress> + Send + 'static {
  pub fn new_worker(&mut self) {
    if self.workers.len() >= self.capacity { return }
    let (tx, rx) = channel();
    let idx = self.workers.len();
    let mut worker = Worker::new(idx, rx, self.mtx.clone());
    self.dispatcher.lock().unwrap().insert(idx, (tx, false));
    worker.spawn();
    self.workers.push(worker);
    let _ = self.mtx.send(Message::NewWorker(idx));
  }
}

impl<E: Copy, I, T: Task<Item=(usize, I)> + Unpin, S: Schedulable<E, T>> Scheduler<E, T, S> {
  fn event_loop(rx: Receiver<Message<E, S::Message, T>>, dispatcher: Arc<Mutex<Dispatcher<E, T>>>, state: Arc<Mutex<S>>) -> Receiver<Message<E, S::Message, T>> {
    while let Ok(m) = rx.recv() {
      match m {
        Message::Finish(i, entry, task) => {
          let mut state = state.lock().unwrap();
          state.restore(entry, task);
          if let Some((entry, task)) = state.fetch() {
          // TODO: dispatch to other thread if failed
            let _ = dispatcher.lock().unwrap().get(&i).unwrap().0.send((entry, task));
          } else {
            dispatcher.lock().unwrap().get_mut(&i).unwrap().1 = true;
          }
        },
        Message::Progress(_, entry, idx, p) => {
          let mut state = state.lock().unwrap();
          state.callback(entry, idx, p.into());
        }
        _ => unimplemented!(),
      }
    }
    rx
  }

  pub fn run(&mut self) {
    let mrx = self.mrx.take().expect("already spawn");
    let state = self.state.clone();
    let dispatcher = self.dispatcher.clone();
    self.mrx = Some(Self::event_loop(mrx, dispatcher, state));
  }
}

impl<E: Copy + Send + 'static, I, T: Task<Item=(usize, I)> + Unpin + Send + 'static, S: Schedulable<E, T> + Send + 'static> Scheduler<E, T, S>
  where S::Message: Send + 'static {
  pub fn spawn(&mut self) {
    if self.handler.is_some() { return }
    let mrx = self.mrx.take().expect("already spawn");
    let state = self.state.clone();
    let dispatcher = self.dispatcher.clone();
    self.handler = Some(std::thread::spawn(|| { Self::event_loop(mrx, dispatcher, state); }));
  }
}
