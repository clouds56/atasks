use std::collections::VecDeque;
use std::future::Future;
use std::task::{Poll, Context, Waker};
use std::pin::Pin;
use std::marker::Unpin;
use futures::{Stream, FutureExt};

pub trait TaskQueueData {
  type Item;
  type Fut: Future + Unpin + Send;
  fn len(&self) -> usize;
  fn check(&mut self, idx: usize, result: &<Self::Fut as Future>::Output) -> bool;
  fn get(&mut self, idx: usize) -> Self::Item;
  fn run(&self, id: Self::Item) -> Self::Fut;
}

pub struct TaskQueue<T: TaskQueueData> {
  idx: usize,
  queue: VecDeque<usize>,
  paused: bool,
  cancelled: bool,
  waker: Option<Waker>,
  data: T,
  current: Vec<(usize, T::Fut)>,
  capacity: usize,
}
impl<T: TaskQueueData + Unpin> Unpin for TaskQueue<T> { }

impl<T: TaskQueueData> TaskQueue<T> {
  fn next_idx(&mut self) -> Option<usize> {
    let idx = if self.idx < self.data.len() {
      Some(self.idx)
    } else {
      self.queue.pop_front()
    };
    self.idx += 1;
    idx
  }
}

impl<T: TaskQueueData> TaskQueue<T> {
  pub fn from_queue(data: T, capacity: usize) -> Self {
    TaskQueue {
      idx: 0,
      queue: VecDeque::new(),
      paused: false,
      waker: None,
      cancelled: false,
      data,
      current: vec![],
      capacity,
    }
  }
  pub fn is_paused(&self) -> bool {
    self.paused
  }
  pub fn is_cancelled(&self) -> bool {
    self.cancelled
  }
}

impl<T: TaskQueueData + Unpin> Task for TaskQueue<T> {
  fn pause(&mut self) { self.paused = true; }
  fn unpause(&mut self) {
    self.paused = false;
    self.waker.as_ref().map(|w| w.wake_by_ref());
  }
  fn cancel(&mut self) { self.cancelled = true; }
}

impl<T: TaskQueueData + Unpin> Stream for TaskQueue<T> {
  type Item = <T::Fut as Future>::Output;
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if self.is_paused() {
      if self.waker.as_ref().map(|w| w.will_wake(cx.waker())) == Some(false) {
        self.waker = Some(cx.waker().clone());
      }
      Poll::Pending
    } else if self.is_cancelled() {
      Poll::Ready(None)
    } else {
      let mut added = false;
      let item = self.current.iter_mut().enumerate().find_map(|(k, (idx, f))| {
        match f.poll_unpin(cx) {
          Poll::Pending => None,
          Poll::Ready(e) => Some((k, *idx, e)),
        }
      }).map(|(k, idx, e)| {
        self.current.swap_remove(k);
        if !self.data.check(idx, &e) {
          self.queue.push_back(idx)
        }
        e
      });
      while self.current.len() < self.capacity {
        if let Some(idx) = self.next_idx() {
          let id = self.data.get(idx);
          let fut = self.data.run(id);
          self.current.push((idx, fut));
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
}

pub trait Task: Stream {
  // type Progress;
  fn pause(&mut self);
  fn unpause(&mut self);
  fn cancel(&mut self);
  // fn dump(&self) -> serde_json::Value;
  // fn restore(v: serde_json::Value) -> Self;
  // fn progress(&self) -> Self::Progress;
}

#[cfg(test)]
mod test {
  use super::*;
  use futures::StreamExt;

  struct Data(usize);
  impl TaskQueueData for Data {
    type Item = usize;
    type Fut = Pin<Box<dyn Future<Output=usize>+Send>>;
    fn len(&self) -> usize { self.0 }
    fn check(&mut self, _idx: usize, _result: &usize) -> bool { true }
    fn get(&mut self, idx: usize) -> Self::Item { idx }
    fn run(&self, id: usize) -> Self::Fut {
      Box::pin(async move { Wake(id).map(|()| id).await })
    }
  }

  #[test]
  fn test_task_queue() {
    futures::executor::block_on(async move {
      // let mut t = HistoryTask::from_queue(HistoryTaskData::new(Id(0), (1..5).map(Id).collect()), 3);
      let mut t = TaskQueue::from_queue(Data(5), 3);
      // println!("{:?}", t.next().await);
      while let Some(i) = t.next().await {
        println!("{:?}", i)
      }
    })
  }

  struct Wake(usize);
  impl Future for Wake {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
      println!("wake {}", self.0);
      if self.0 == 0 {
        Poll::Ready(())
      } else {
        self.0 -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
      }
    }
  }

  #[test]
  fn test_async() {
    futures::executor::block_on(async move {
      Wake(5).await
    })
  }
}
