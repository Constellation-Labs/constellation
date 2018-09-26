package org.constellation

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives._
import com.google.common.util.concurrent.RateLimiter

object CustomDirectives {

  object Limiters {
    private var rateLimiter: Option[RateLimiter] = None
    def getInstance(tps: Double): RateLimiter = rateLimiter match {
      case Some(rateLimiter) ⇒ rateLimiter
      case None ⇒
        rateLimiter = Some(RateLimiter.create(tps))
        rateLimiter.get
    }
  }

  trait Throttle {
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

  trait BannedIPEnforcer {

    val parentNode: ConstellationNode


    def rejectBannedIP: Directive0 = {
      extractClientIP flatMap { ip =>
        if (parentNode.IPManager.knownIP(ip)) {
          complete(StatusCodes.Forbidden)
        } else {
          pass
        }
      }
    }

    def enforceKnownIP: Directive0 = {
      extractClientIP flatMap { ip =>
        if (parentNode.IPManager.knownIP(ip)) {
          pass
        } else {
          // Initiate signing flow

          complete(StatusCodes.Forbidden)
        }
      }
    }
  }

}
