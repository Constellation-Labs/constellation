package org.constellation.domain.redownload

import cats.implicits._
import org.constellation.domain.redownload.RedownloadService.SnapshotsAtHeight

case class RedownloadPlan(
  toDownload: SnapshotsAtHeight,
  toRemove: SnapshotsAtHeight,
  toLeave: SnapshotsAtHeight,
) {
  // TODO: Is it the proper place/way of calculating the expected state of acceptedSnapshots after redownload?
  lazy val expectedResult: SnapshotsAtHeight =
    toLeave |+| toDownload
}
