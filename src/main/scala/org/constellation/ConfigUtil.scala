package org.constellation

import java.lang
import java.util.concurrent.TimeUnit

import cats.data.NonEmptyList
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object ConfigUtil {

  val config: Config = ConfigFactory.load().resolve()
  val constellation = config.getConfig("constellation")
  private val configAuth = config.getConfig("auth")
  private val configStorage = config.getConfig("constellation.storage")

  val snapshotSizeDiskLimit: lang.Long =
    Try(config.getBytes("constellation.snapshot-size-disk-limit")).getOrElse(java.lang.Long.valueOf(1000000))

  val snapshotClosestFractionSize: Int = Try(config.getInt("constellation.snapshot-closest-fraction-size"))
    .getOrElse(12)

  val maxNestedCBresolution: Int = Try(config.getInt("constellation.max-nested-cb-resolution"))
    .getOrElse(100)

  def getOrElse(path: String, default: Int): Int =
    Try(config.getInt(path)).getOrElse(default)

  def getOrElse(path: String, default: Long): Long =
    Try(config.getLong(path)).getOrElse(default)

  def getOrElse(path: String, default: Boolean): Boolean =
    Try(config.getBoolean(path)).getOrElse(default)

  def get(path: String): Try[String] = Try(config.getString(path))

  def getDurationFromConfig(path: String): FiniteDuration =
    FiniteDuration(config.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)

  def getDurationFromConfig(path: String, default: FiniteDuration, ownConfig: Config = config): FiniteDuration =
    Try(FiniteDuration(ownConfig.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))
      .getOrElse(default)

  def getAuthEnabled: Boolean =
    configAuth.getBoolean("enabled")

  def getAuthId: String =
    configAuth.getString("id")

  def getAuthPassword: String =
    configAuth.getString("password")

  def isEnabledGCPStorage: Boolean =
    Try(configStorage.getBoolean("gcp.enabled")).getOrElse(false)

  def isEnabledAWSStorage: Boolean =
    Try(configStorage.getBoolean("aws.enabled")).getOrElse(false)

  case class AWSStorageConfig(accessKey: String, secretKey: String, region: String, bucket: String)

  def loadAWSStorageConfigs(constellationConfig: Config = constellation): NonEmptyList[AWSStorageConfig] = {
    val accessKey = constellationConfig.getString(s"storage.aws.aws-access-key")
    val secretKey = constellationConfig.getString(s"storage.aws.aws-secret-key")
    val region = constellationConfig.getString(s"storage.aws.region")

    def load(bucket: String): AWSStorageConfig =
      AWSStorageConfig(
        accessKey = accessKey,
        secretKey = secretKey,
        region = region,
        bucket = bucket
      )

    val backupStorage: List[String] = Try(
      constellationConfig.getStringList("storage.aws.backup-buckets-names").asScala.toList
    ).getOrElse(Nil)

    val base = load(constellationConfig.getString(s"storage.aws.bucket-name"))
    val backups = backupStorage.map(load)

    NonEmptyList(base, backups)
  }
}
