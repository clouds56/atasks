use futures::Stream;

pub trait AsProgress {
  type Progress;
  fn progress(&self) -> Self::Progress;
  // fn dump(&self) -> serde_json::Value;
  // fn restore(v: serde_json::Value) -> Self;
}

#[delegatable_trait]
pub trait Control: AsProgress {
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

pub trait Task: Stream {
  type Controller: Control;
  fn controller(&self) -> Self::Controller;
}

pub struct Progress {
  pub current: usize,
  pub completed: usize,
  pub failed: usize,
  pub processed: Option<usize>,
  pub total: usize,
}
