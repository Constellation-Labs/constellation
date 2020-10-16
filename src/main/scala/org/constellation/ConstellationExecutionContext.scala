package org.constellation

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent._

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, Concurrent, ContextShift, IO, Resource, SyncIO}

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

  val unboundedResource: Resource[SyncIO, ExecutionContext] =
    Resource
      .make(SyncIO {
        new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          60L,
          TimeUnit.SECONDS,
          new SynchronousQueue[Runnable],
          new DefaultThreadFactory("unbounded")
        )
      })(es => SyncIO(es.shutdown()))
      .map(ExecutionContext.fromExecutor)

  val boundedResource: Resource[IO, ExecutionContext] =
    Resource
      .make(
        IO(
          new ForkJoinPool({
            val available = Runtime.getRuntime.availableProcessors
            if (available == 1) 1 else available - 1
          }, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true)
        )
      )(es => IO(es.shutdown()))
      .map(ExecutionContext.fromExecutor)

  val callbacksResource: Resource[IO, ExecutionContext] = Resource
    .make(
      IO(
        new ThreadPoolExecutor(
          4,
          4,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue[Runnable],
          new DefaultThreadFactory("callbacks")
        )
      )
    )(es => IO(es.shutdown()))
    .map(ExecutionContext.fromExecutor)

  val callbacksHealthResource: Resource[IO, ExecutionContext] = Resource
    .make(
      IO(
        new ThreadPoolExecutor(
          2,
          2,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue[Runnable],
          new DefaultThreadFactory("callbacks-health")
        )
      )
    )(es => IO(es.shutdown()))
    .map(ExecutionContext.fromExecutor)

  val unboundedHealthResource: Resource[IO, ExecutionContext] = Resource
    .make(
      IO(
        new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE,
          60L,
          TimeUnit.SECONDS,
          new SynchronousQueue[Runnable],
          new DefaultThreadFactory("unbounded-health")
        )
      )
    )(es => IO(es.shutdown()))
    .map(ExecutionContext.fromExecutor)

//  val unboundedBlocker: Blocker = Blocker.liftExecutionContext(unbounded)
//  val unboundedHealthBlocker: Blocker = Blocker.liftExecutionContext(unboundedHealth)

  def createSemaphore[F[_]: Concurrent](permits: Long = 1): Semaphore[F] =
    Semaphore.in[IO, F](permits).unsafeRunSync()

}
