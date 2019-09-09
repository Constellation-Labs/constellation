package org.constellation.util

import cats.effect.{Bracket, Clock, IO, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.ConstellationExecutionContext

import scala.concurrent.duration._

object Logging {

  implicit val timer = IO.timer(ConstellationExecutionContext.unbounded)

  def clock[F[_]: Clock] = Clock[F].realTime(MILLISECONDS)

  def logThread[F[_]: Sync: Clock, A](
    fa: F[A],
    operationName: String
  )(implicit L: Logger[F], B: Bracket[F, Throwable]): F[A] =
    B.bracket(
      startLog(operationName) *> clock
    )(_ => fa)(clk1 => clock.flatMap(finishedLog(operationName, clk1, _)))

  def logThread[A](fa: IO[A], operationName: String)(implicit L: Logger[IO]): IO[A] =
    (startLog(operationName) *> clock[IO])
      .bracket(_ => fa)(clk1 => clock[IO].flatMap(finishedLog(operationName, clk1, _)))

  def logThread[A](fa: IO[A], operationName: String, logger: com.typesafe.scalalogging.Logger): IO[A] =
    (IO(startLog(operationName, logger)) *> clock[IO])
      .bracket(_ => fa)(clk1 => clock[IO].flatMap(clk2 => IO(finishedLog(operationName, clk1, clk2, logger))))

  private def startLog[F[_]](operationName: String)(implicit L: Logger[F]) =
    L.debug(s"Starting [${operationName}] on thread: ${Thread.currentThread.getName}")

  private def startLog(operationName: String, logger: com.typesafe.scalalogging.Logger) =
    logger.debug(s"Starting [${operationName}] on thread: ${Thread.currentThread.getName}")

  private def finishedLog[F[_]](operationName: String, clk1: Long, clk2: Long)(implicit L: Logger[F]) =
    L.debug(s"Finished [${operationName}] on thread: ${Thread.currentThread.getName} and took ${clk2 - clk1}ms")

  private def finishedLog(operationName: String, clk1: Long, clk2: Long, logger: com.typesafe.scalalogging.Logger) =
    logger.debug(s"Finished [${operationName}] on thread: ${Thread.currentThread.getName} and took ${clk2 - clk1}ms")
}
