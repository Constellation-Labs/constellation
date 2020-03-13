package org.constellation.infrastructure.rewards

import cats.effect.Concurrent
import org.constellation.domain.cloud.{HeightHashFileStorage, S3Storage}
import org.constellation.domain.rewards.StoredEigenTrust

class EigenTrustS3Storage[F[_]: Concurrent](accessKey: String, secretKey: String, region: String, bucket: String)
    extends S3Storage[F, StoredEigenTrust](accessKey, secretKey, region, bucket, Some("snapshots"))
    with HeightHashFileStorage[F, StoredEigenTrust] {
  override val fileSuffix: String = "eigen_trust"
}

object EigenTrustS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): EigenTrustS3Storage[F] = new EigenTrustS3Storage[F](accessKey, secretKey, region, bucket)
}
