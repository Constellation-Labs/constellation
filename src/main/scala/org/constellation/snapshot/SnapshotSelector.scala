package org.constellation.snapshot

import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import org.constellation.schema.Id
import org.constellation.p2p.PeerData
import org.constellation.storage.{RecentSnapshot, SnapshotVerification}
import org.constellation.util.SnapshotDiff

abstract class SnapshotSelector[F[_]: Concurrent] {

  type SelectionInfo = (SnapshotDiff, List[RecentSnapshot])
  type PeersSnapshots = Map[Id, List[RecentSnapshot]]

  def selectSnapshotFromRecent(
    peersSnapshots: PeersSnapshots,
    ownSnapshots: List[RecentSnapshot]
  ): Option[SelectionInfo]

  def selectSnapshotFromBroadcastResponses(
    responses: List[Option[SnapshotVerification]],
    ownSnapshots: List[RecentSnapshot]
  ): Option[SelectionInfo]

  def collectSnapshot(
    peers: Map[Id, PeerData]
  )(cs: ContextShift[F])(implicit m: Manifest[List[RecentSnapshot]]): F[Map[Id, List[RecentSnapshot]]] =
    peers.toList.traverse { peer =>
      peer._2.client.getNonBlockingF[F, List[RecentSnapshot]]("snapshot/recent")(cs).map((peer._1, _))
    }.map(_.toMap)

}
