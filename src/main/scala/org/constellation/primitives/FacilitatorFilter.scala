package org.constellation.primitives

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.p2p.PeerData
import org.constellation.schema.Id

import scala.util.Random

class FacilitatorFilter[F[_]: Concurrent](
  apiClient: ClientInterpreter[F],
  dao: DAO
) {

  val logger = Slf4jLogger.getLogger[F]

  def filterPeers(peers: Map[Id, PeerData], numFacilitatorPeers: Int, tipSoe: TipSoe): F[Map[Id, PeerData]] =
    for {
      minTipHeight <- tipSoe.minHeight.getOrElse(0L).pure[F]
      _ <- logger.debug(s"[${dao.id.short}] : [Facilitator Filter] : selected minTipHeight = $minTipHeight")

      filteredPeers <- filterByHeight(Random.shuffle(peers.toList), minTipHeight, numFacilitatorPeers)
      peerIds = filteredPeers.map(_._1)
      _ <- logger.debug(s"[${dao.id.short}] : [Facilitator Filter] : $peerIds : size = ${peerIds.size}")
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
        _ <- logger.debug(
          s"[${dao.id.short}] : [Facilitator Filter] : Checking facilitator with next snapshot height : $facilitatorHeight"
        )
        height = facilitatorHeight._2
      } yield if (height <= ownHeight + 2) result :+ peer else result

      checkHeight.flatMap(updatedResult => filterByHeight(filteredPeers, ownHeight, numFacilitatorPeers, updatedResult))
    }

  private def getFacilitatorNextSnapshotHeights(facilitator: (Id, PeerData)): F[(Id, Long)] =
    apiClient.snapshot.getNextSnapshotHeight().run(facilitator._2.peerMetadata.toPeerClientMetadata)
}
