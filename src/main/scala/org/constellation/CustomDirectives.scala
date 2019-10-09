package org.constellation

import java.net.InetSocketAddress

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{HttpRequest, RemoteAddress, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.{Directive, Directive0, Directive1, RouteResult}
import cats.effect.IO
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.constellation.primitives.IPManager
import org.constellation.util.APIDirective

import scala.util.Try

object CustomDirectives {

  def printResponseTime(logger: Logger)(log: LoggingAdapter) = {
    val requestTimestamp = System.nanoTime

    wrapper(logger, requestTimestamp) _
  }

  def wrapper(logger: Logger, requestTimestamp: Long)(req: HttpRequest)(res: RouteResult) =
    res match {
      case Complete(resp) =>
        val responseTimestamp: Long = System.nanoTime
        val elapsedTime: Long = (responseTimestamp - requestTimestamp) / 1000000
        val loggingString =
          s"""Logged Request:${req.method}:${req.uri}:${resp.status}:${elapsedTime}ms"""
        logger.info(loggingString)
      case Rejected(reason) =>
        logger.info(s"Rejected Reason: ${reason.mkString(",")}")
    }

  trait Throttle {

    def throttle(socketAddress: InetSocketAddress, tps: Double): Directive0 =
      APIDirective.extractIP(socketAddress).flatMap { ip =>
        val rateLimiter = Limiters.getInstance(tps)
        if (rateLimiter.tryAcquire(1)) {
          pass
        } else {
          complete(StatusCodes.TooManyRequests)
        }
      }
  }

  trait IPEnforcer extends StrictLogging {

    val ipManager: IPManager[IO]

    def rejectBannedIP(ip: String): Directive0 = {
      val isBannedIP = ipManager.bannedIP(ip).unsafeRunSync

      if (isBannedIP) {
        logger.info(s"Reject banned ip: $ip")
        complete(StatusCodes.Forbidden)
      } else {
        pass
      }
    }

    def enforceKnownIP(ip: String): Directive0 = {
      val isKnownIP = ipManager.knownIP(ip).unsafeRunSync

      if (isKnownIP) {
        pass
      } else {
        logger.info(s"Reject unknown ip: $ip")
        complete(
          StatusCodes.custom(403, "ip unknown. Need to register using the /register endpoint.")
        )
      }
    }
  }

  object Limiters {
    private var rateLimiter: Option[RateLimiter] = None

    def getInstance(tps: Double): RateLimiter = rateLimiter match {
      case Some(limiter) ⇒ limiter
      case None ⇒
        rateLimiter = Some(RateLimiter.create(tps))
        rateLimiter.get
    }
  }

}
