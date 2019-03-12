package org.constellation
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object ConfigUtil {

  val config: Config = ConfigFactory.load().resolve()

  def getDurationFromConfig(path: String): FiniteDuration = {
    FiniteDuration(config.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
  def getDurationFromConfig(path: String, default: FiniteDuration): FiniteDuration = {
    Try(FiniteDuration(config.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      .getOrElse(default)
  }
}
