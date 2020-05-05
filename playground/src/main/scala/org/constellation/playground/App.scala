package org.constellation.playground

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._

object App extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = ExitCode.Success.pure[IO]
}
