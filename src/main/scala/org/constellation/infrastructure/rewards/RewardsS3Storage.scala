package org.constellation.infrastructure.rewards

import cats.effect.Concurrent
import org.constellation.domain.cloud.{HeightHashFileStorage, S3Storage}
import org.constellation.domain.rewards.StoredRewards

class RewardsS3Storage[F[_]: Concurrent](accessKey: String, secretKey: String, region: String, bucket: String)
    extends S3Storage[F, StoredRewards](accessKey, secretKey, region, bucket, Some("snapshots"))
    with HeightHashFileStorage[F, StoredRewards] {
  override val fileSuffix: String = "eigen_trust"
}

object RewardsS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): RewardsS3Storage[F] = new RewardsS3Storage[F](accessKey, secretKey, region, bucket)
}
