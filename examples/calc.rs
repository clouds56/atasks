extern crate atasks;

use atasks::queue::{TaskQueue, TaskQueueData};
use atasks::core::*;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Context};
use futures::StreamExt;

struct Calc {
  data: f64,
  inner: Pin<Box<dyn Future<Output=Result<f64, ()>>+Send>>,
}
impl Calc {
  fn new(i: f64) -> Self {
    Self { data: i, inner: Box::pin(Self::run(i)) }
  }
  async fn run(i: f64) -> Result<f64, ()> {
    use rand::Rng;
    let t = (i * 20.0 % 1000.0) as u64;
    async_std::task::sleep(std::time::Duration::from_millis(t)).await;
    let p: u32 = rand::thread_rng().gen_range(0, 100);
    if p < 80 {
      return Err(())
    }
    Ok(i * i)
  }
}
impl Future for Calc {
  type Output=Option<(f64, f64)>;
  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.inner.as_mut().poll(cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(Err(_)) => Poll::Ready(None),
      Poll::Ready(Ok(s)) => Poll::Ready(Some((self.data, s))),
    }
  }
}

struct CalcQueueData {
  data: Vec<f64>,
  visit_next: HashSet<usize>,
  visit_check: HashSet<usize>,
  result: f64,
}

impl TaskQueueData for CalcQueueData {
  type Item = f64;
  type Fut = Calc;
  fn size_hint(&self) -> (usize, Option<usize>) {
    let len = self.data.len();
    (len, Some(len))
  }
  fn check(&mut self, idx: usize, result: &Option<(f64, f64)>) -> bool {
    if let Some(result) = result {
      assert_eq!(result.0, self.data[idx]);
      self.result += result.1;
      assert!(self.visit_check.remove(&idx));
      true
    } else {
      assert!(self.visit_check.contains(&idx));
      // println!("{} failed and retry", idx);
      false
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
    Self { data, visit_check: (0..len).collect(), visit_next: (0..len+1).collect(), result: 0. }
  }
  fn gen_queue(len: usize, capacity: usize) -> TaskQueue<Self> {
    TaskQueue::from_queue(CalcQueueData::gen(len), capacity)
  }
  fn calc(&self) -> f64 {
    self.data.iter().fold(0.0, |acc, &x| acc + x*x)
  }
  fn check(&self, p: Progress) {
    assert!((self.calc() - self.result).abs() < 1e-6);
    assert!(self.visit_check.is_empty());
    assert!(self.visit_next.is_empty());
    assert_eq!(p.current, 0);
    assert_eq!(p.failed, 0);
    assert_eq!(p.completed, self.data.len());
    assert_eq!(p.total, p.completed);
    assert!(p.processed.unwrap() > p.completed);
    println!("{:?}", p);
  }
}

fn main() {
  let mut s = CalcQueueData::gen_queue(100, 5);
  let s_ref = &mut s;
  futures::executor::block_on(async move {
    while let Some(_) = s_ref.next().await { }
  });
  s.data().check(s.controller().progress());
}
