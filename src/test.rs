use crate::queue::*;
use std::task::{Poll, Context};
use std::pin::Pin;
use futures::{Future, FutureExt};

#[derive(Default)]
pub struct Counter(pub usize);
impl<T> std::iter::FromIterator<T> for Counter {
  fn from_iter<I: std::iter::IntoIterator<Item = T>>(iter: I) -> Self {
    Self(iter.into_iter().fold(0, |acc, _| acc+1))
  }
}
impl<T> std::iter::Extend<T> for Counter {
  fn extend<I: std::iter::IntoIterator<Item = T>>(&mut self, iter: I) {
    self.0 += iter.into_iter().fold(0, |acc, _| acc+1)
  }
}

pub struct Wake(pub usize);
impl Future for Wake {
  type Output = ();
  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    // println!("wake {}", self.0);
    if self.0 == 0 {
      Poll::Ready(())
    } else {
      self.0 -= 1;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  }
}

pub struct QueueData(pub usize);
impl TaskQueueData for QueueData {
  type Item = usize;
  type Fut = Pin<Box<dyn Future<Output=usize>+Send>>;
  fn size_hint(&self) -> (usize, Option<usize>) { (self.0, Some(self.0)) }
  fn next(&mut self, idx: usize) -> (usize, Option<Self::Item>) {
    (idx, if idx < self.0 { Some(idx) } else { None })
   }
  fn run(&self, &id: &usize) -> Self::Fut {
    Box::pin(async move { Wake(id).map(|()| id).await })
  }
}
