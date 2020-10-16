package org.constellation.util

import java.io.{PrintWriter, StringWriter}

import cats.effect.{Bracket, Clock, IO, Sync, Timer}
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import org.constellation.ConstellationExecutionContext

import scala.concurrent.duration._

object Logging {

  def stringifyStackTrace(e: Throwable): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  def clock[F[_]: Clock]: F[Long] = Clock[F].realTime(MILLISECONDS)

  def logThread[F[_]: Sync: Clock, A](
    fa: F[A],
    operationName: String
  )(implicit L: Logger[F], B: Bracket[F, Throwable]): F[A] =
    B.bracket(
      startLog(operationName) >> clock
    )(_ => fa)(clk1 => clock.flatMap(finishedLog(operationName, clk1, _)))

  def logThread[A](fa: IO[A], operationName: String)(implicit L: Logger[IO], T: Timer[IO]): IO[A] =
    (startLog(operationName) >> clock[IO])
      .bracket(_ => fa)(clk1 => clock[IO].flatMap(finishedLog(operationName, clk1, _)))

  def logThread[A](fa: IO[A], operationName: String, logger: com.typesafe.scalalogging.Logger)(
    implicit T: Timer[IO]
  ): IO[A] =
    (IO(startLog(operationName, logger)) >> clock[IO])
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
