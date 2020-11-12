package org.constellation

import cats.effect.{ExitCode, IO, IOApp}

object TestApp extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    IO.pure(ExitCode.Success)

}
