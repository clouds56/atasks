use crate::core::*;

use std::task::{Poll, Context};
use std::pin::Pin;
use futures::Stream;

pub struct Controller(Box<dyn Control>);

impl Controller {
  pub fn from<T: Control + 'static>(controller: T) -> Self {
    Self(Box::new(controller))
  }
}
impl Control for Controller {
  ambassador_impl_Control_body_single_struct!{0}
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
impl<C: Control + 'static, I> Task for TaskBox<C, I> {
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
  pub fn from<C: Control, I, T: Task<Controller=C, Item=(usize, I)>>(task: T) -> Self
    where C: 'static, I: 'static, T: 'static {
    Self(TaskBox(Box::pin(TaskBox::<C, I>(Box::pin(task)))))
  }
}
