use std::pin::Pin;

use futures::{Stream, Future};

#[derive(Debug, Clone)]
pub struct Progress {
  pub current: usize,
  pub completed: usize,
  pub failed: usize,
  pub processed: Option<usize>,
  pub total: usize,
}

#[derive(Debug)]
pub struct State {
  pub idx: usize,
  pub items: usize, // success_items + failed_items + retried_items + current_items
  pub current: usize, // current_items
      // completed = success_items = processed - current - failed - retries
  pub failed: usize, // failed_items
  pub retries: usize, // => retried_items = items - completed - current - fail
  pub total_hint: usize,
  pub total: Option<usize>,
  pub processed: usize, // current + failed + retries + completed
  pub paused: bool,
  pub stopped: bool,
}

#[delegatable_trait]
pub trait Control {
  fn progress(&self) -> Progress;
  fn pause(&mut self);
  fn unpause(&mut self);
  fn stop(&mut self);
  fn resume(&mut self);
  fn shutdown(&mut self);
  fn is_paused(&self) -> bool;
  fn is_stopped(&self) -> bool;
  fn is_shutdown(&self) -> bool;
  fn is_running(&self) -> bool;
  fn is_finished(&self) -> bool;
}

impl State {
  pub fn new(size_hint: (usize, Option<usize>)) -> Self {
    Self {
      idx: 0, items: 0, current: 0, failed: 0, retries: 0,
      processed: 0, total_hint: size_hint.0, total: None,
      paused: false, stopped: false, }
  }
  pub fn total(&self) -> usize {
    self.total.unwrap_or(self.total_hint)
  }
  pub fn completed(&self) -> usize {
    self.processed - self.current - self.failed - self.retries
  }
}

impl Control for State {
  fn progress(&self) -> Progress {
    Progress {
      current: self.current,
      completed: self.completed(),
      failed: self.failed,
      processed: Some(self.processed),
      total: self.total(),
    }
  }

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
    self.completed() + self.failed == self.items
  }
}

pub trait Job: Stream {
  type Controller: Control;
  fn controller(&self) -> Self::Controller;
}

#[async_trait]
pub trait Task: Sized + Send + 'static {
  // type Item = Self;
  type Output: 'static;
  async fn run(_: Self) -> Self::Output;
  fn boxed(self) -> Pin<Box<dyn Future<Output=Self::Output>+Send>> {
    Box::pin(Self::run(self))
  }
}
pub type FutureOf<T> = Pin<Box<dyn Future<Output=<T as Task>::Output>+Send>>;

#[async_trait]
impl<T: 'static> Task for Pin<Box<dyn Future<Output=T>+Send>> {
  type Output = T;
  async fn run(s: Self) -> T {
    s.await
  }
  fn boxed(self) -> Self {
    self
  }
}
