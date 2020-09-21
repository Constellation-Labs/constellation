package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.domain.cloud.{HeightHashFileStorage, S3Storage}
import org.constellation.schema.snapshot.StoredSnapshot

class SnapshotS3Storage[F[_]: Concurrent](accessKey: String, secretKey: String, region: String, bucket: String)
    extends S3Storage[F, StoredSnapshot](accessKey, secretKey, region, bucket, Some("snapshots"))
    with HeightHashFileStorage[F, StoredSnapshot] {

  override val fileSuffix: String = "snapshot"
}

object SnapshotS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): SnapshotS3Storage[F] = new SnapshotS3Storage[F](accessKey, secretKey, region, bucket)
}
