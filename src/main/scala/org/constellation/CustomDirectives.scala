package org.constellation

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.{Directive0, RouteResult}
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.scalalogging.Logger

import org.constellation.primitives.IPManager

/** Custom directives ??. */
object CustomDirectives {

  /** Limiters ??. */
  object Limiters {
    private var rateLimiter: Option[RateLimiter] = None

    /** RateLimiter getter.
      *
      * @param tps ... Transactions per second.
      * @return RateLimiter instance.
      */
    def getInstance(tps: Double): RateLimiter = rateLimiter match {
      case Some(rateLimiter) ⇒ rateLimiter
      case None ⇒
        rateLimiter = Some(RateLimiter.create(tps))
        rateLimiter.get
    }
  }

  /** Throttle trait ??. */
  trait Throttle {

    /** ??.
      *
      * @param tps ... Transactions per second.
      * @return Directive0 ??.
      */
    def throttle(tps: Double): Directive0 =
      extractClientIP flatMap { ip =>
        val rateLimiter = Limiters.getInstance(tps)
        if (rateLimiter.tryAcquire(1)) {
          pass
        } else {
          complete(StatusCodes.TooManyRequests)
        }
      }
  }

  /** Throttle trait. */
  trait IPEnforcer {

    val ipManager: IPManager

    /** @return Directive0 ??. */
    def rejectBannedIP: Directive0 = {
      extractClientIP flatMap { ip =>

        println(s"Reject banned ip: ${
          ip.toOption.map {
            _.getHostAddress
          }
        } ${
          ip.toIP.map {
            _.ip.getHostAddress
          }
        }")
        if (ipManager.bannedIP(ip)) {
          complete(StatusCodes.Forbidden)
        } else {
          pass
        }
      }
    }

    /** @return Directive0 ??. */
    def enforceKnownIP: Directive0 = {
      extractClientIP flatMap { ip =>
        if (ipManager.knownIP(ip)) {
          pass
        } else {
          complete(StatusCodes.custom(403, "ip unknown. Need to register using the /register endpoint."))
        }
      }
    }
  }

  /** ??.
    *
    * @constructor .
    * @param logger           ... Logger instance.
    * @param requestTimestamp ... Timestamp of request made.
    * @param req              ... http request.
    * @param res              ... Route result ??.
    */
  def wrapper(logger: Logger, requestTimestamp: Long)(req: HttpRequest)(res: RouteResult) = res match {
    case Complete(resp) =>
      val responseTimestamp: Long = System.nanoTime
      val elapsedTime: Long = (responseTimestamp - requestTimestamp) / 1000000
      val loggingString =
        s"""Logged Request:${req.method}:${req.uri}:${resp.status}:${elapsedTime}ms"""
      logger.info(loggingString)
    case Rejected(reason) =>
      logger.info(s"Rejected Reason: ${reason.mkString(",")}")
  }

  /** ??.
    *
    * @constructor .
    * @param logger ... Logger instance.
    * @param log    ... ??.
    */
  def printResponseTime(logger: Logger)(log: LoggingAdapter) = {
    val requestTimestamp = System.nanoTime

    wrapper(logger, requestTimestamp) _
  }

}

