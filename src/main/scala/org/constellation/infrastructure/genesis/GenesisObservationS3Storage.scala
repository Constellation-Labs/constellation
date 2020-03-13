package org.constellation.infrastructure.genesis

import cats.effect.Concurrent
import org.constellation.domain.cloud.S3Storage
import org.constellation.primitives.Schema.GenesisObservation

class GenesisObservationS3Storage[F[_]: Concurrent](
  accessKey: String,
  secretKey: String,
  region: String,
  bucket: String
) extends S3Storage[F, GenesisObservation](accessKey, secretKey, region, bucket) {}

object GenesisObservationS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): GenesisObservationS3Storage[F] = new GenesisObservationS3Storage[F](accessKey, secretKey, region, bucket)
}
