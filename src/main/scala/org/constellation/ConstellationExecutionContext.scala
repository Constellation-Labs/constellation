package org.constellation

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{
  Executors,
  ForkJoinPool,
  LinkedBlockingQueue,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}

import scala.concurrent.ExecutionContext

object ConstellationExecutionContext {

  /*
  // By default number of available cpus use for CPU-intensive work
  val bounded = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())
  //  an unbounded thread pool for executing blocking I/O calls
  val unbounded = ExecutionContext.fromExecutor(Executors.newCachedThreadPool()
  //  a bounded thread pool for non-blocking I/O callbacks
  val callbacks = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
   */

  def threadFactory(prefix: String): ThreadFactory = new ThreadFactory() {
    private val threadIndex = new AtomicLong(0)

    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable)
      thread.setName(s"${prefix}-" + threadIndex.getAndIncrement())
      thread
    }
  }

  val bounded = ExecutionContext.fromExecutor(
    new ForkJoinPool(
      Runtime.getRuntime.availableProcessors,
      ForkJoinPool.defaultForkJoinWorkerThreadFactory,
      null,
      true
    )
  )

  val unbounded = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      60L,
      TimeUnit.SECONDS,
      new SynchronousQueue[Runnable],
      threadFactory("unbounded")
    )
  )

  val callbacks = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      4,
      4,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory("callbacks")
    )
  )
}
