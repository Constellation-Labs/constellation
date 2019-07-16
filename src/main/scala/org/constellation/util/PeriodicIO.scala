package org.constellation.util
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

abstract class PeriodicIO[T](threadName: String, periodSeconds: Int = 1) extends StrictLogging {
  private val executor = new ScheduledThreadPoolExecutor(1)

  var round: Long = 0L

  var lastExecution: Future[T] = Future.failed(new RuntimeException("No executions yet"))

  def trigger(): IO[T]

  private val task = new Runnable {

    def run(): Unit = {
      round += 1
      Thread.currentThread().setName(threadName)
      logger.debug(s"triggering ${threadName} round: $round last execution was completed: ${lastExecution.isCompleted}")
      if (lastExecution.isCompleted) {
        lastExecution = trigger().unsafeToFuture
      }
    }
  }

  private val scheduledFuture = executor.scheduleAtFixedRate(task, 1, periodSeconds, TimeUnit.SECONDS)

  def shutdown(): Boolean = scheduledFuture.cancel(false)
}
