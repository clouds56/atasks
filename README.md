atasks
=======

* task: a task is an async runnable that `Future` that computes from `Item` to `Output`
* job: a job is a collection of similar tasks, usually `TaskQueue<T: TaskQueueData>`
* worker: receives task and run
* scheduler: dispatch task from jobs to worker
