use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::cell::Cell;

use crate::core::*;

pub(crate) use crossbeam_channel::{unbounded as channel, Sender, Receiver};

pub trait Schedulable<E, T> {
  fn flush(&mut self);
  fn restore(&mut self, entry: E, task: T);
  fn fetch(&mut self) -> Option<(E, T)>;
}

pub enum Message<E, P, T> {
  Progress(usize, E, usize, P),
  Finish(usize, E, T),
  Flush,
}

pub struct Worker<E, P, T> {
  idx: usize,
  rx: Receiver<(E, T)>,
  tx: Sender<Message<E, P, T>>,
}

pub struct Scheduler<E, P, T, S> {
  mrx: Cell<Option<Receiver<Message<E, P, T>>>>,
  mtx: Sender<Message<E, P, T>>,
  capacity: usize,
  dispatcher: Arc<Mutex<HashMap<usize, Sender<(E, T)>>>>,
  workers: Vec<Worker<E, P, T>>,
  state: Arc<Mutex<S>>,
}

impl<E: Copy, I, T: Task<Item=(usize, I)> + Unpin> Worker<E, <T::Controller as AsProgress>::Progress, T> {
  pub fn run(&self) {
    use futures::StreamExt;
    futures::executor::block_on(async {
      while let Ok((entry, mut t)) = self.rx.recv() {
        let controller = t.controller();
        while let Some((i, _)) = t.next().await {
          let _ = self.tx.send(Message::Progress(self.idx, entry, i, controller.progress()));
        }
        let _ = self.tx.send(Message::Finish(self.idx, entry, t));
      }
    });
  }
}

impl<E, P, T, S> Scheduler<E, P, T, S> {
  pub fn new(capacity: usize, state: Arc<Mutex<S>>) -> Self {
    let (mtx, mrx) = channel();
    Self {
      mtx, mrx: Cell::new(Some(mrx)), state, capacity,
      dispatcher: Arc::new(Mutex::new(HashMap::new())),
      workers: Vec::new(),
    }
  }
}

impl<E: Copy + Send + 'static, I, P, T: Task<Item=(usize, I)> + Unpin + Send + 'static, S: Schedulable<E, T> + Send + 'static> Scheduler<E, P, T, S>
  where P: Into<<T::Controller as AsProgress>::Progress> + From<<T::Controller as AsProgress>::Progress> + Send + 'static {
  pub fn run(rx: Receiver<Message<E, P, T>>, dispatcher: Arc<Mutex<HashMap<usize, Sender<(E, T)>>>>, state: Arc<Mutex<S>>) {
    while let Ok(m) = rx.recv() {
      match m {
        Message::Finish(i, entry, task) => {
          let mut state = state.lock().unwrap();
          state.restore(entry, task);
          if let Some((entry, task)) = state.fetch() {
          // TODO: dispatch to other thread if failed
            let _ = dispatcher.lock().unwrap().get(&i).unwrap().send((entry, task));
          }
        }
        _ => unimplemented!(),
      }
    }
  }

  pub fn spawn(&self) {
    let mrx = self.mrx.take().expect("already spawn");
    let state = self.state.clone();
    let dispatcher = self.dispatcher.clone();
    let hanlder = std::thread::spawn(|| Self::run(mrx, dispatcher, state));
  }
}
