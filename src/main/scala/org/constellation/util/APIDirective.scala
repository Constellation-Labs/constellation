package org.constellation.util

import cats.effect.IO
import akka.http.scaladsl.server.{Directive1, Directives}
import org.constellation.ConstellationExecutionContext.{bounded, callbacks}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object APIDirective {

  private def eval[A](ioa: IO[A], ec: ExecutionContext): Future[A] =
    IO.contextShift(callbacks).evalOn(ec)(ioa).unsafeToFuture()

  def handle[A](
    ioa: IO[A],
    ec: ExecutionContext = bounded
  ): Directive1[A] =
    Directives.onComplete(eval(ioa, ec)).flatMap {
      case Success(response) => Directives.provide(response)
      case Failure(error)    => Directives.complete(error)
    }

  def onHandle[A](
    ioa: IO[A],
    ec: ExecutionContext = bounded
  ): Directive1[Try[A]] =
    Directives.onComplete(eval(ioa, ec))

}
