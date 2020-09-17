package org.constellation

import com.google.common.util.concurrent.RateLimiter

object CustomDirectives {

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
