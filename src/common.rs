use crate::core::*;

use std::task::{Poll, Context};
use std::pin::Pin;
use futures::Stream;

#[derive(Delegate)]
#[delegate(Control)]
struct ControllerBox<P: Into<Progress>>(Box<dyn Control<Progress=P>>);
impl<P: Into<Progress>> AsProgress for ControllerBox<P> {
  type Progress = Progress;
  fn progress(&self) -> Progress { self.0.progress().into() }
}
#[derive(Delegate)]
#[delegate(Control)]
pub struct Controller(ControllerBox<Progress>);
impl AsProgress for Controller {
  type Progress = Progress;
  fn progress(&self) -> Progress { self.0.progress() }
}
impl Controller {
  pub fn from<P: Into<Progress> + 'static, T: Control<Progress=P> + 'static>(controller: T) -> Self {
    Self(ControllerBox(Box::new(ControllerBox::<P>(Box::new(controller)))))
  }
}

// SomeController => ControllerBox<P> => ControllerBox<Progress>

struct TaskBox<C: Control, I>(Pin<Box<dyn Task<Controller=C, Item=(usize, I)>>>);

impl<C: Control, I> Stream for TaskBox<C, I> {
  type Item = (usize, ());
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self.0.as_mut().poll_next(cx) {
      Poll::Ready(Some((idx, _))) => Poll::Ready(Some((idx, ()))),
      Poll::Pending => Poll::Pending,
      Poll::Ready(None) => Poll::Ready(None),
    }
  }
}
impl<P: Into<Progress> + 'static, C: Control<Progress=P> + 'static, I> Task for TaskBox<C, I> {
  type Controller = Controller;
  fn controller(&self) -> Controller { Controller::from(self.0.controller()) }
}

pub struct GeneralTask(TaskBox<Controller, ()>);
impl Stream for GeneralTask {
  type Item = (usize, ());
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    (self.0).0.as_mut().poll_next(cx)
  }
}
impl Task for GeneralTask {
  type Controller = Controller;
  fn controller(&self) -> Controller { self.0.controller() }
}
impl GeneralTask {
  pub fn from<P: Into<Progress>, C: Control<Progress=P>, I, T: Task<Controller=C, Item=(usize, I)>>(task: T) -> Self
    where P: 'static, C: 'static, I: 'static, T: 'static {
    Self(TaskBox(Box::pin(TaskBox::<C, I>(Box::pin(task)))))
  }
}
