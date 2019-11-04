package org.constellation.snapshot

import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import org.constellation.schema.Id
import org.constellation.p2p.PeerData
import org.constellation.storage.SnapshotVerification
import org.constellation.util.SnapshotDiff

abstract class SnapshotSelector[F[_]: Concurrent, T <: AnyRef] {

  type SnapshotSelectionInput = T

  type NodeSnapshots = (Id, SnapshotSelectionInput)
  type SelectionInfo = (SnapshotDiff, SnapshotSelectionInput)

  def selectSnapshotFromRecent(
    peersSnapshots: List[NodeSnapshots],
    ownSnapshots: SnapshotSelectionInput
  ): Option[SelectionInfo]

  def selectSnapshotFromBroadcastResponses(
    responses: List[Option[SnapshotVerification]],
    ownSnapshots: SnapshotSelectionInput
  ): Option[SelectionInfo]

  def collectSnapshot(
    peers: Map[Id, PeerData]
  )(cs: ContextShift[F])(implicit m: Manifest[SnapshotSelectionInput]): F[List[(Id, SnapshotSelectionInput)]] =
    peers.toList.traverse(
      p => (p._1, p._2.client.getNonBlockingF[F, SnapshotSelectionInput]("snapshot/recent")(cs)).sequence
    )

}
