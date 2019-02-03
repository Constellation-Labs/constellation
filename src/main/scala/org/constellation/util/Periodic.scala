package org.constellation.util

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.Future
import scala.util.Try

abstract class Periodic(threadName: String, periodSeconds: Int = 1) {

  private val executor = new ScheduledThreadPoolExecutor(1)

  var round: Long = 0L

  private var lastExecution : Future[Any] = Future.successful(())

  /** Documentation. */
  def trigger(): Future[Any]
  /**
    * Recalculates window based / periodic metrics
    */
  private val task = new Runnable {

    /** Documentation. */
    def run(): Unit = {
      round += 1
      Thread.currentThread().setName(threadName)
      if (lastExecution.isCompleted) {
        lastExecution = trigger()
      }
    }
  }

  // We may get rid of Akka so using this instead of the context scheduler
  private val scheduledFuture = executor.scheduleAtFixedRate(task, 1, periodSeconds, TimeUnit.SECONDS)

  /** Documentation. */
  def shutdown(): Boolean = scheduledFuture.cancel(false)

}

