package org.constellation

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

object ConstellationExecutionContext {
  // By default number of available cpus use for CPU-intensive work
  val bounded = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())
  //  an unbounded thread pool for executing blocking I/O calls
  val unbounded = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  //  a bounded thread pool for non-blocking I/O callbacks
  val callbacks = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
}
