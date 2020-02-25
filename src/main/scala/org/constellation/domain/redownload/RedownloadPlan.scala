package org.constellation.domain.redownload

import org.constellation.domain.redownload.RedownloadService.SnapshotsAtHeight

case class RedownloadPlan(
  toDownload: SnapshotsAtHeight,
  toRemove: SnapshotsAtHeight,
  toLeave: SnapshotsAtHeight
) {}
