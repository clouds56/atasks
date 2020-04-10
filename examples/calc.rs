extern crate atasks;

use atasks::queue::{TaskQueue, TaskQueueData, TaskResult};
use atasks::tasks::PriorityTasks;
use atasks::core::*;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Context};

struct Calc {
  data: f64,
  inner: Pin<Box<dyn Future<Output=Result<f64, bool>>+Send>>,
}
impl Calc {
  fn new(i: f64) -> Self {
    Self { data: i, inner: Box::pin(Self::run(i)) }
  }
  async fn run(i: f64) -> Result<f64, bool> {
    use rand::Rng;
    let t = (i * 20.0 % 1000.0) as u64;
    async_std::task::sleep(std::time::Duration::from_millis(t)).await;
    let p: u32 = rand::thread_rng().gen_range(0, 100);
    if p < 5 {
      return Err(true)
    } else if p < 20 {
      return Err(false)
    }
    Ok(i * i)
  }
}
impl Future for Calc {
  type Output=Result<(f64, f64), bool>;
  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.inner.as_mut().poll(cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(result) => Poll::Ready(result.map(|s| (self.data, s))),
    }
  }
}

struct CalcQueueData {
  data: Vec<f64>,
  visit_next: HashSet<usize>,
  visit_check: HashSet<usize>,
  failed: HashSet<usize>,
  result: f64,
}

impl TaskQueueData for CalcQueueData {
  type Item = f64;
  type Fut = Calc;
  fn size_hint(&self) -> (usize, Option<usize>) {
    let len = self.data.len();
    (len, Some(len))
  }
  fn check(&mut self, idx: usize, result: &Result<(f64, f64), bool>) -> TaskResult {
    match result {
      Ok((i, i2)) => {
        assert_eq!(*i, self.data[idx]);
        self.result += *i2;
        assert!(self.visit_check.remove(&idx));
        TaskResult::Success
      },
      Err(true) => {
        assert!(self.visit_check.contains(&idx));
        assert!(self.failed.insert(idx));
        TaskResult::Failed
      },
      Err(false) => {
        assert!(self.visit_check.contains(&idx));
        // println!("{} failed and retry", idx);
        TaskResult::Retry
      }
    }
  }
  fn next(&mut self, idx: usize) -> (usize, Option<Self::Item>) {
    assert!(self.visit_next.remove(&idx), "revisit {}", idx);
    if idx >= self.data.len() { return (self.data.len(), None) }
    let item = self.data[idx];
    (idx, Some(item))
  }
  fn run(&self, &item: &f64) -> Self::Fut {
    Calc::new(item)
  }
}

impl CalcQueueData {
  fn gen(len: usize) -> Self {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let data = (0..len).map(|_| rng.gen()).collect();
    Self { data, visit_check: (0..len).collect(), visit_next: (0..len+1).collect(), failed: (0..0).collect(), result: 0. }
  }
  fn gen_queue(len: usize, capacity: usize) -> TaskQueue<Self> {
    TaskQueue::from_queue(CalcQueueData::gen(len), capacity)
  }
  fn calc(&self) -> f64 {
    self.data.iter().enumerate().filter(|(i,_)| !self.failed.contains(i)).fold(0.0, |acc, (_, x)| acc + x*x)
  }
  fn check_result(&self, p: Progress) {
    assert_eq!(self.visit_check, self.failed);
    assert!((self.calc() - self.result).abs() < 1e-6);
    assert!(self.visit_next.is_empty());
    assert_eq!(p.current, 0);
    assert_eq!(p.total, self.data.len());
    assert_eq!(p.failed + p.completed, p.total);
    assert!(p.processed.unwrap() > p.total);
    println!("{:?}", p);
  }
}

fn main() {
  dotenv::dotenv().ok();
  env_logger::init();
  let mut tasks = PriorityTasks::<i32, TaskQueue<CalcQueueData>>::new(3, None);
  tasks.schedule();
  tasks.update(|t| {
    (0..30).map(|_| t.add_task_resume(0, CalcQueueData::gen_queue(100, 5))).collect::<Vec<_>>()
  });
  tasks.wait();
  let finished = tasks.finished();
  assert_eq!(finished.len(), 30);
  finished.iter().for_each(|(_, s)| s.data().check_result(s.controller().progress()));
}
