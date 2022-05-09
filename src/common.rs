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

struct JobBox<C: Control, I>(Pin<Box<dyn Job<Controller=C, Item=(usize, I)>>>);

impl<C: Control, I> Stream for JobBox<C, I> {
  type Item = (usize, ());
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    match self.0.as_mut().poll_next(cx) {
      Poll::Ready(Some((idx, _))) => Poll::Ready(Some((idx, ()))),
      Poll::Pending => Poll::Pending,
      Poll::Ready(None) => Poll::Ready(None),
    }
  }
}
impl<C: Control + 'static, I> Job for JobBox<C, I> {
  type Controller = Controller;
  fn controller(&self) -> Controller { Controller::from(self.0.controller()) }
}

pub struct GeneralJob(JobBox<Controller, ()>);
impl Stream for GeneralJob {
  type Item = (usize, ());
  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    (self.0).0.as_mut().poll_next(cx)
  }
}
impl Job for GeneralJob {
  type Controller = Controller;
  fn controller(&self) -> Controller { self.0.controller() }
}
impl GeneralJob {
  pub fn from<C: Control, I, T: Job<Controller=C, Item=(usize, I)>>(job: T) -> Self
    where C: 'static, I: 'static, T: 'static {
    Self(JobBox(Box::pin(JobBox::<C, I>(Box::pin(job)))))
  }
}
