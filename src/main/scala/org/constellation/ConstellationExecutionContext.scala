package org.constellation

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent._

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, Concurrent, ContextShift, IO}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

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

  val bounded: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())

  val unbounded: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      60L,
      TimeUnit.SECONDS,
      new SynchronousQueue[Runnable],
      new DefaultThreadFactory("unbounded")
    )
  )

  val callbacks: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      4,
      4,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      new DefaultThreadFactory("callbacks")
    )
  )

  val callbacksHealth: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    new ThreadPoolExecutor(
      2,
      2,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      new DefaultThreadFactory("callbacks-health")
    )
  )

  val unboundedHealth: ExecutionContextExecutor = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(
      4,
      new DefaultThreadFactory("unbounded-health")
    )
  )

  val unboundedBlocker: Blocker = Blocker.liftExecutionContext(unbounded)
  val unboundedHealthBlocker: Blocker = Blocker.liftExecutionContext(unboundedHealth)

  def createSemaphore[F[_]: Concurrent](permits: Long = 1): Semaphore[F] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(bounded)
    Semaphore.in[IO, F](permits).unsafeRunSync()
  }

}
