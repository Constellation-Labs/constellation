package org.constellation.primitives

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.p2p.PeerData
import org.constellation.domain.schema.Id

import scala.util.Random

class FacilitatorFilter[F[_]: Concurrent: Logger](calculationContext: ContextShift[F], dao: DAO) {

  def filterPeers(peers: Map[Id, PeerData], numFacilitatorPeers: Int, tipSoe: TipSoe): F[Map[Id, PeerData]] =
    for {
      minTipHeight <- tipSoe.minHeight.getOrElse(0L).pure[F]
      _ <- Logger[F].debug(s"[${dao.id.short}] : [Facilitator Filter] : selected minTipHeight = $minTipHeight")

      filteredPeers <- filterByHeight(Random.shuffle(peers.toList), minTipHeight, numFacilitatorPeers)
      peerIds = filteredPeers.map(_._1)
      _ <- Logger[F].debug(s"[${dao.id.short}] : [Facilitator Filter] : $peerIds : size = ${peerIds.size}")
    } yield peers.filter(peer => peerIds.contains(peer._1))

  private def filterByHeight(
    peers: List[(Id, PeerData)],
    ownHeight: Long,
    numFacilitatorPeers: Int = 2,
    result: List[(Id, PeerData)] = List.empty[(Id, PeerData)]
  ): F[List[(Id, PeerData)]] =
    if (peers.isEmpty || result.size == numFacilitatorPeers) {
      result.pure[F]
    } else {
      val peer = peers.head
      val filteredPeers = peers.filterNot(_ == peer)
      val checkHeight = for {
        facilitatorHeight <- getFacilitatorNextSnapshotHeights(peer)
        _ <- Logger[F].debug(
          s"[${dao.id.short}] : [Facilitator Filter] : Checking facilitator with next snapshot height : $facilitatorHeight"
        )
        height = facilitatorHeight._2
      } yield if (height <= ownHeight + 2) result :+ peer else result

      checkHeight.flatMap(updatedResult => filterByHeight(filteredPeers, ownHeight, numFacilitatorPeers, updatedResult))
    }

  private def getFacilitatorNextSnapshotHeights(facilitator: (Id, PeerData)): F[(Id, Long)] =
    facilitator._2.client.getNonBlockingF[F, (Id, Long)]("snapshot/nextHeight")(calculationContext)
}
