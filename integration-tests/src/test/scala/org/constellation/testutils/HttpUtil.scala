package org.constellation.testutils

import cats.effect.{ContextShift, IO, Timer}
import fs2.{Pipe, Stream}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.{EntityDecoder, Request}

import scala.concurrent.ExecutionContext.global

object HttpUtil {

  def evalRequestForHosts[T](hosts: List[String])(
    pipe: Pipe[IO, String, IO[Request[IO]]]
  )(implicit ev: EntityDecoder[IO, T], timer: Timer[IO], cs: ContextShift[IO]): List[T] =
    BlazeClientBuilder[IO](global).resource.use { httpClient =>
      Stream
        .emits(hosts)
        .through(pipe)
        .evalMap { req =>
          httpClient.expect[T](req)
        }
        .compile
        .toList
    }.unsafeRunSync()

  def evalRequest[T](
    req: IO[Request[IO]]
  )(implicit ev: EntityDecoder[IO, T], timer: Timer[IO], cs: ContextShift[IO]): T =
    BlazeClientBuilder[IO](global).resource.use { httpClient =>
      httpClient
        .expect[T](req)
    }.unsafeRunSync()

}
