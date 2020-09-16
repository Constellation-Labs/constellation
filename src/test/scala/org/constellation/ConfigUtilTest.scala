package org.constellation

import cats.data.NonEmptyList
import com.typesafe.config.ConfigValue
import org.constellation.ConfigUtil.AWSStorageConfig
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ConfigUtilTest extends AnyFreeSpec with Matchers {
  "should correctly load AWS storage config when buckup storages are specified" in {
    val expected = NonEmptyList(
      AWSStorageConfig("awsAccessKey", "awsSecret", "region", "bucketA"),
      List(
        AWSStorageConfig("awsAccessKey", "awsSecret", "region", "bucketB"),
        AWSStorageConfig("awsAccessKey", "awsSecret", "region", "bucketC")
      )
    )

    val result = ConfigUtil.loadAWSStorageConfigs()

    result shouldBe expected
  }

  "should correctly load AWS storage config when buckup storages are not specified" in {
    val expected = NonEmptyList(
      AWSStorageConfig("awsAccessKey", "awsSecret", "region", "bucketA"),
      Nil
    )

    val result =
      ConfigUtil.loadAWSStorageConfigs(ConfigUtil.constellation.withoutPath("storage.aws.backup-buckets-names"))

    result shouldBe expected
  }
}
