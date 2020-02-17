package org.constellation.domain.redownload

import cats.effect.Concurrent
import org.constellation.schema.Id

class MajorityStateChooser[F[_]](implicit F: Concurrent[F]) {

  // TODO: Use RedownloadService type definitions
  def chooseMajorityState(
    createdSnapshots: Map[Long, String],
    peersProposals: Map[Id, Map[Long, String]]
  ): Map[Long, String] = ???
}

object MajorityStateChooser {
  def apply[F[_]: Concurrent](): MajorityStateChooser[F] = new MajorityStateChooser[F]()
}
