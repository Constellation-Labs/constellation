package org.constellation

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives._
import com.google.common.util.concurrent.RateLimiter
import org.constellation.primitives.IPManager

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

  trait IPEnforcer {

    val ipManager: IPManager


    def rejectBannedIP: Directive0 = {
      extractClientIP flatMap { ip =>
        if (ipManager.bannedIP(ip)) {
          complete(StatusCodes.Forbidden)
        } else {
          pass
        }
      }
    }

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

}
