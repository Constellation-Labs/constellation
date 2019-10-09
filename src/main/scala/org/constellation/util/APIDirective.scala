package org.constellation.util

import java.net.InetSocketAddress

import cats.effect.IO
import akka.http.scaladsl.server.{Directive1, Directives}
import org.constellation.ConstellationExecutionContext.{bounded, callbacks, unbounded}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object APIDirective {

  private def eval[A](ioa: IO[A], ec: ExecutionContext): Future[A] =
    IO.contextShift(unbounded).evalOn(ec)(ioa).unsafeToFuture()

  def handle[A](
    ioa: IO[A],
    ec: ExecutionContext = unbounded
  ): Directive1[A] =
    Directives.onComplete(eval(ioa, ec)).flatMap {
      case Success(response) => Directives.provide(response)
      case Failure(error)    => Directives.complete(error)
    }

  def onHandle[A](
    ioa: IO[A],
    ec: ExecutionContext = unbounded
  ): Directive1[Try[A]] =
    Directives.onComplete(eval(ioa, ec))

  def extractIP(
    socketAddress: InetSocketAddress
  ): Directive1[String] = Directives.extractClientIP.map { extractedIP =>
    extractedIP.toOption
      .map(_.getHostAddress)
      .getOrElse(socketAddress.getAddress.getHostAddress)
  }

  def extractHostPort(
    socketAddress: InetSocketAddress
  ): Directive1[HostPort] = Directives.extractClientIP.map { extractedIP =>
    extractedIP.toOption
      .map(ip => HostPort(ip.getHostAddress, extractedIP.getPort))
      .getOrElse(HostPort(socketAddress.getAddress.getHostAddress, socketAddress.getPort))
  }

}
