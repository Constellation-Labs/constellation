package org.constellation.storage

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import org.constellation.p2p.PeerData
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache, FinishedCheckpoint, TipData}
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.schema.{Height, Id, checkpoint}
import scala.util.Random

case class TipSoe(soe: Seq[SignedObservationEdge], minHeight: Option[Long])

object TipSoe {
  implicit val tipSoeEncoder: Encoder[TipSoe] = deriveEncoder
  implicit val tipSoeDecoder: Decoder[TipSoe] = deriveDecoder
}

case class PulledTips(tipSoe: TipSoe, peers: Map[Id, PeerData])
case class TipConflictException(cb: CheckpointBlock, conflictingTxs: List[String])
    extends Exception(
      s"CB with baseHash: ${cb.baseHash} is conflicting with other tip or its ancestor. With following txs: $conflictingTxs"
    )
case class TipThresholdException(cb: CheckpointBlock, limit: Int)
    extends Exception(
      s"Unable to add CB with baseHash: ${cb.baseHash} as tip. Current tips limit met: $limit"
    )
