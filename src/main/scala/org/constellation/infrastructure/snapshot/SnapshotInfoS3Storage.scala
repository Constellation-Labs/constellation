package org.constellation.infrastructure.snapshot

import cats.effect.Concurrent
import org.constellation.domain.cloud.{HeightHashFileStorage, S3Storage}
import org.constellation.schema.snapshot.SnapshotInfo

class SnapshotInfoS3Storage[F[_]: Concurrent](accessKey: String, secretKey: String, region: String, bucket: String)
    extends S3Storage[F, SnapshotInfo](accessKey, secretKey, region, bucket, Some("snapshots"))
    with HeightHashFileStorage[F, SnapshotInfo] {

  override val fileSuffix: String = "snapshot_info"
}

object SnapshotInfoS3Storage {

  def apply[F[_]: Concurrent](
    accessKey: String,
    secretKey: String,
    region: String,
    bucket: String
  ): SnapshotInfoS3Storage[F] = new SnapshotInfoS3Storage[F](accessKey, secretKey, region, bucket)
}
