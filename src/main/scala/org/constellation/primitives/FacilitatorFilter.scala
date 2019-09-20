package org.constellation.primitives

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.consensus.TipData
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.{Height, Id}

class FacilitatorFilter[F[_]: Concurrent: Logger](calculationContext: ContextShift[F], dao: DAO) {

  def filterPeers(
    peers: Map[Id, PeerData],
    ownTips: Map[String, TipData],
    numFacilitatorPeers: Int
  ): F[Map[Id, PeerData]] =
    for {
      ownHeights <- LiftIO[F].liftIO(ownHeight(ownTips))
      ownHeight = maxOrZero(ownHeights.map(_.min))
      _ <- Logger[F].info(s"[${dao.id.short}] : [Facilitator Filter] : ownHeight = $ownHeight")

      filteredPeers <- filterByHeight(peers.toList, ownHeight, numFacilitatorPeers)
      peerIds = filteredPeers.map(_._1)
      _ <- Logger[F].info(s"[${dao.id.short}] : [Facilitator Filter] : $peerIds : size = ${peerIds.size}")
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
        heights <- getFacilitatorHeights(peer)
        height = maxOrZero(heights._2.map(_.min))
      } yield if (height <= ownHeight + 2) result :+ peer else result

      checkHeight.flatMap(updatedResult => filterByHeight(filteredPeers, ownHeight, numFacilitatorPeers, updatedResult))
    }

  private def ownHeight(tips: Map[String, TipData]): IO[List[Height]] =
    tips.toList
      .traverse(t => dao.checkpointService.lookup(t._1))
      .map(maybeHeights => maybeHeights.flatMap(_.flatMap(_.height)))

  private def getFacilitatorHeights(facilitator: (Id, PeerData)): F[(Id, List[Height])] =
    facilitator._2.client
      .getNonBlockingF[F, List[Height]]("heights")(calculationContext)
      .map(heights => (facilitator._1, heights))

  private def maxOrZero(heights: List[Long]): Long =
    heights match {
      case Nil  => 0L
      case list => list.max
    }
}
