package org.constellation.genesis

import cats.data.EitherT
import cats.effect.Concurrent
import org.constellation.domain.cloud.S3Storage
import org.constellation.schema.v2.GenesisObservation

class GenesisObservationS3Storage[F[_]: Concurrent](
  accessKey: String,
  secretKey: String,
  region: String,
  bucket: String
) extends S3Storage[F, GenesisObservation](accessKey, secretKey, region, bucket) {

  def write(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit] =
    write("genesisObservation", genesisObservation)

  def read(): EitherT[F, Throwable, GenesisObservation] =
    read("genesisObservation")
}

object GenesisObservationS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): GenesisObservationS3Storage[F] = new GenesisObservationS3Storage[F](accessKey, secretKey, region, bucket)
}
