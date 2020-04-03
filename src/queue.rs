use std::collections::VecDeque;
use std::future::Future;
use std::task::{Poll, Context, Waker};
use std::pin::Pin;
use std::marker::Unpin;
use std::sync::{Arc, Mutex};
use std::cell::Cell;
use futures::{Stream, FutureExt};

use crate::core::*;

// it is like Iterator, but not the same
pub trait TaskQueueData {
  type Item;
  type Fut: Future + Unpin + Send;
  fn size_hint(&self) -> (usize, Option<usize>);
  fn check(&mut self, idx: usize, result: &<Self::Fut as Future>::Output) -> bool;
  fn next(&mut self, idx: usize) -> (usize, Option<Self::Item>);
  fn run(&self, id: &Self::Item) -> Self::Fut;
}

pub struct TaskQueue<T: TaskQueueData> {
  state: Arc<Mutex<State>>,
  current: Vec<(usize, T::Item, T::Fut)>,
  queue: VecDeque<(usize, T::Item)>,
  waker: Cell<Option<Waker>>,
  data: T,
  capacity: usize,
}
impl<T: TaskQueueData + Unpin> Unpin for TaskQueue<T> { }

impl<T: TaskQueueData> TaskQueue<T> {
  fn next_idx(&mut self) -> Option<(usize, T::Item)> {
    let mut state = self.state.lock().unwrap();
    if state.total.is_none() || state.idx < state.total.unwrap() {
      let (idx, item) = self.data.next(state.idx);
      if let Some(item) = item {
        state.items += 1;
        state.idx = idx + 1;
        return Some((idx, item))
      } else {
        state.total = Some(idx)
      }
    }
    self.queue.pop_front()
  }
}

impl<T: TaskQueueData> TaskQueue<T> {
  pub fn from_queue(data: T, capacity: usize) -> Self {
    TaskQueue {
      state: Arc::new(Mutex::new(State::new(data.size_hint()))),
      current: vec![],
      queue: VecDeque::new(),
      waker: Cell::new(None),
      data,
      capacity,
    }
  }
  pub fn data(&self) -> &T { &self.data }
  fn update_waker(&self, new_waker: &Waker) {
    if let Some(waker) = self.waker.take() {
      if waker.will_wake(new_waker) {
        self.waker.set(Some(waker));
        return
      }
    }
    self.waker.set(Some(new_waker.clone()));
  }
  fn current_push(&mut self, idx: usize, item: T::Item, fut: T::Fut) {
    let mut state = self.state.lock().unwrap();
    self.current.push((idx, item, fut));
    state.current += 1;
    state.processed += 1;
  }
  fn current_pop(&mut self, k: usize, check: bool) {
    let mut state = self.state.lock().unwrap();
    let (idx, item, _) = self.current.swap_remove(k);
    if !check {
      self.queue.push_back((idx, item));
      state.failed += 1;
    }
    state.current -= 1;
  }
  pub fn is_paused(&self) -> bool {
    self.state.lock().unwrap().paused
  }
  pub fn is_stopped(&self) -> bool {
    self.state.lock().unwrap().stopped
  }
  pub fn is_finished(&self) -> bool {
    self.state.lock().unwrap().is_finished()
  }
  pub fn total_hint(&self) -> usize {
    self.state.lock().unwrap().total()
  }
  pub fn is_cancelled(&self) -> bool {
    let state = self.state.lock().unwrap();
    state.paused && state.stopped
  }
}

impl<T: TaskQueueData + Unpin> Task for TaskQueue<T> {
  type Controller = Controller;
  fn controller(&self) -> Self::Controller {
    Controller(self.state.clone())
  }
}

impl<T: TaskQueueData + Unpin> Stream for TaskQueue<T> {
  type Item = (usize, <T::Fut as Future>::Output);
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if let Ok(state) = self.state.lock() {
      if state.is_paused() {
        if state.is_stopped() {
          return Poll::Ready(None)
        }
        self.update_waker(cx.waker());
        return Poll::Pending
      }
    }
    let mut added = false;
    let item = self.current.iter_mut().enumerate().find_map(|(k, (idx, _, f))| {
      match f.poll_unpin(cx) {
        Poll::Pending => None,
        Poll::Ready(e) => Some((k, *idx, e)),
      }
    }).map(|(k, idx, e)| {
      let check = self.data.check(idx, &e);
      self.current_pop(k, check);
      (idx, e)
    });
    while !self.is_stopped() && self.current.len() < self.capacity {
      if let Some((idx, item)) = self.next_idx() {
        let fut = self.data.run(&item);
        self.current_push(idx, item, fut);
        added = true;
      } else {
        break
      }
    }
    if let Some(e) = item {
      Poll::Ready(Some(e))
    } else if self.current.is_empty() {
      Poll::Ready(None)
    } else {
      if added {
        cx.waker().wake_by_ref();
      }
      Poll::Pending
    }
  }
}

struct State {
  idx: usize,
  items: usize, // count next(), = success_items + failed_items + current_items
  current: usize, // = current_items, completed = success_items
  failed: usize, // => failed_items = items - completed - current
  total_hint: usize,
  total: Option<usize>,
  processed: usize, // = current + failed + completed
  paused: bool,
  stopped: bool,
}
// TODO: https://github.com/hobofan/ambassador/issues/20
// #[derive(Delegate)]
// #[delegate(Control, target = "self.0.lock().unwrap()")]
pub struct Controller(Arc<Mutex<State>>);

impl State {
  fn new(size_hint: (usize, Option<usize>)) -> Self {
    Self {
      idx: 0, items: 0, current: 0, failed: 0,
      processed: 0, total_hint: size_hint.0, total: None,
      paused: false, stopped: false, }
  }
  fn total(&self) -> usize {
    self.total.unwrap_or(self.total_hint)
  }
}
impl AsProgress for State {
  type Progress = Progress;
  fn progress(&self) -> Self::Progress {
    let completed = self.processed - self.current - self.failed;
    let failed = self.items - completed - self.current;
    Progress {
      current: self.current,
      completed, failed,
      processed: Some(self.processed),
      total: self.total(),
    }
  }
}
impl Control for State {
  fn pause(&mut self) { self.paused = true }
  fn unpause(&mut self) {
    if !self.stopped { self.paused = false }
  }
  fn stop(&mut self) { self.stopped = true }
  fn resume(&mut self) {
    if !self.paused { self.stopped = false }
   }
  fn shutdown(&mut self) { self.paused = true; self.stopped = true }
  fn is_paused(&self) -> bool { self.paused }
  fn is_stopped(&self) -> bool { self.stopped }
  fn is_shutdown(&self) -> bool { self.paused && self.stopped }
  fn is_running(&self) -> bool { !self.paused && !self.stopped }
  fn is_finished(&self) -> bool {
    !self.paused && !self.stopped &&
    self.current == 0 &&
    Some(self.idx) == self.total &&
    self.failed == self.processed - self.items
  }
}

impl AsProgress for Controller {
  type Progress = Progress;
  fn progress(&self) -> Self::Progress { self.0.lock().unwrap().progress() }
}
impl Control for Controller {
  // TODO delegate
  fn pause(&mut self) { self.0.lock().unwrap().pause() }
  fn unpause(&mut self) { self.0.lock().unwrap().unpause() }
  fn stop(&mut self) { self.0.lock().unwrap().stop() }
  fn resume(&mut self) { self.0.lock().unwrap().resume() }
  fn shutdown(&mut self) { self.0.lock().unwrap().shutdown() }
  fn is_paused(&self) -> bool { self.0.lock().unwrap().is_paused() }
  fn is_stopped(&self) -> bool { self.0.lock().unwrap().is_stopped() }
  fn is_shutdown(&self) -> bool { self.0.lock().unwrap().is_shutdown() }
  fn is_running(&self) -> bool { self.0.lock().unwrap().is_running() }
  fn is_finished(&self) -> bool { self.0.lock().unwrap().is_finished() }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::test::*;
  use futures::StreamExt;

  #[test]
  fn test_task_queue() {
    futures::executor::block_on(async move {
      // let mut t = HistoryTask::from_queue(HistoryTaskData::new(Id(0), (1..5).map(Id).collect()), 3);
      let mut t = TaskQueue::from_queue(QueueData(5), 3);
      // println!("{:?}", t.next().await);
      let mut k = 0;
      while let Some(i) = t.next().await {
        k += 1;
        println!("{:?}", i);
        assert_eq!(!t.is_finished(), k < 5);
      }
      assert_eq!(k, 5);
    })
  }

  #[test]
  fn test_task_stop() {
    futures::executor::block_on(async move {
      let mut t = TaskQueue::from_queue(QueueData(5), 3);
      let mut controller = t.controller();
      assert_eq!(t.is_stopped(), false);
      controller.stop();
      assert_eq!(t.is_stopped(), true);
      assert!(t.next().await.is_none());
      assert_eq!(t.is_finished(), false);
      controller.resume();
      assert_eq!(t.is_stopped(), false);
      assert_eq!((&mut t).collect::<Counter>().await.0, 5);
      assert_eq!(t.is_finished(), true);
    })
  }

  #[test]
  fn test_async() {
    futures::executor::block_on(async move {
      Wake(5).await
    })
  }
}
