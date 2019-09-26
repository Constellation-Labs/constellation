package org.constellation.primitives

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema.Id

class FacilitatorFilter[F[_]: Concurrent: Logger](calculationContext: ContextShift[F], dao: DAO) {

  def filterPeers(peers: Map[Id, PeerData], numFacilitatorPeers: Int): F[Map[Id, PeerData]] =
    for {
      ownHeight <- LiftIO[F].liftIO(ownHeight())
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
        facilitatorHeight <- getFacilitatorHeights(peer)
        _ <- Logger[F].info(
          s"[${dao.id.short}] : [Facilitator Filter] : Checking facilitator with height : $facilitatorHeight"
        )
        height = facilitatorHeight._2
      } yield if (height <= ownHeight + 2) result :+ peer else result

      checkHeight.flatMap(updatedResult => filterByHeight(filteredPeers, ownHeight, numFacilitatorPeers, updatedResult))
    }

  private def ownHeight(): IO[Long] =
    dao.concurrentTipService.getMinTipHeight(None)

  private def getFacilitatorHeights(facilitator: (Id, PeerData)): F[(Id, Long)] =
    facilitator._2.client.getNonBlockingF[F, (Id, Long)]("heights/min")(calculationContext)
}
