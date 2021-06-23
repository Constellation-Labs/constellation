package org.constellation.util

import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import cats.effect.{ContextShift, IO, Timer}
import com.typesafe.scalalogging.StrictLogging
import org.constellation.ConstellationExecutionContext

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

abstract class PeriodicIO(taskName: String, taskExecutionContext: ExecutionContext) extends StrictLogging {

  val timerPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  val taskPool: ExecutionContext = taskExecutionContext
  implicit val contextShift: ContextShift[IO] = IO.contextShift(taskPool)
  implicit val timer: Timer[IO] = IO.timer(timerPool)

  val executionNumber: AtomicLong = new AtomicLong(0)
  val cancellationToken: AtomicBoolean = new AtomicBoolean(false)

  def trigger(): IO[Unit]

  def schedule(initialDelay: FiniteDuration, duration: FiniteDuration): Unit =
    timer
      .sleep(initialDelay)
      .unsafeRunAsync {
        case Left(_)  => logger.error(s"Unexpected error while triggering periodic task ${taskName} with initial delay")
        case Right(_) => schedule(duration)
      }

  def schedule(duration: FiniteDuration): Unit = {
    val delayedTask = timer
      .sleep(duration)
//      .flatMap(_ => IO(logger.debug(s"triggering periodic task ${taskName}")))
      .flatMap(
        _ =>
          IO.contextShift(timerPool)
            .evalOn(taskPool)(trigger().handleErrorWith { ex =>
              IO(logger.error(s"Error when executing periodic task: $taskName due: ${ex.getMessage}", ex))
            })
      )
    delayedTask
      .unsafeToFuture()
      .onComplete { res =>
        val currNumber = executionNumber.incrementAndGet()
//        logger.debug(s"Periodic task: $taskName has finished $res execution number: $currNumber")
        if (!cancellationToken.get()) schedule(duration)
      }(timerPool)
  }

  def cancel(): IO[Unit] = IO {
    // TODO: shutdown timerPool
    cancellationToken.set(true)
  }
}
