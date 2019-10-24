package org.constellation

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{
  Executors,
  ForkJoinPool,
  LinkedBlockingQueue,
  SynchronousQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}

import cats.effect.{Concurrent, ContextShift, IO}
import cats.effect.concurrent.Semaphore

import scala.concurrent.ExecutionContext

sealed class DefaultThreadFactory(prefix: String) extends ThreadFactory {
  private val poolNumber: AtomicInteger = new AtomicInteger(1)
  private val threadNumber: AtomicInteger = new AtomicInteger(1)

  val s: SecurityManager = System.getSecurityManager
  val group: ThreadGroup = if (s != null) s.getThreadGroup else Thread.currentThread().getThreadGroup
  val namePrefix: String = prefix + "-pool-" + poolNumber.getAndIncrement() + "-thread-"

  def newThread(r: Runnable): Thread = {
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
    if (t.isDaemon)
      t.setDaemon(false);
    if (t.getPriority != Thread.NORM_PRIORITY)
      t.setPriority(Thread.NORM_PRIORITY);

    t
  }
}

object ConstellationExecutionContext {

  /*
  // By default number of available cpus use for CPU-intensive work
  val bounded = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())
  //  an unbounded thread pool for executing blocking I/O calls
  val unbounded = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  //  a bounded thread pool for non-blocking I/O callbacks
  val callbacks = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
   */

  val bounded = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())

  val unbounded = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      60L,
      TimeUnit.SECONDS,
      new SynchronousQueue[Runnable],
      new DefaultThreadFactory("unbounded")
    )
  )

  val callbacks = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      4,
      4,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      new DefaultThreadFactory("callbacks")
    )
  )

  def createSemaphore[F[_]: Concurrent](permits: Long = 1): Semaphore[F] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(bounded)
    Semaphore.in[IO, F](permits).unsafeRunSync()
  }

}
