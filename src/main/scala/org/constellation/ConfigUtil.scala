package org.constellation
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object ConfigUtil {

  val config: Config = ConfigFactory.load().resolve()

  val snapshotSizeDiskLimit: Long = Try(config.getLong("constellation.snapshot-size-disk-limit"))
    .getOrElse(26214400)

  val snapshotClosestFractionSize: Int = Try(config.getInt("constellation.snapshot-closest-fraction-size"))
    .getOrElse(50)

  val maxNestedCBresolution: Int = Try(config.getInt("constellation.max-nested-cb-resolution"))
    .getOrElse(100)

  def getOrElse(path: String, default: Int): Int = {
    Try(config.getInt(path))
      .getOrElse(default)
  }
  def getDurationFromConfig(path: String): FiniteDuration = {
    FiniteDuration(config.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  }
  def getDurationFromConfig(path: String, default: FiniteDuration): FiniteDuration = {
    Try(FiniteDuration(config.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      .getOrElse(default)
  }
}
