package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema._
import constellation._

object EdgeService {

  def createCheckpointEdge(activeTips: Seq[TypedEdgeHash], memPool: Seq[String])(implicit keyPair: KeyPair): CheckpointEdge = {
    // take those transactions bundle and sign them
    // TODO: temp logic
    val tip1 = activeTips.head
    val tip2 = activeTips.last

    val cb = CheckpointEdgeData(memPool)

    val oe = ObservationEdge(
      TypedEdgeHash(tip1.hash, EdgeHashType.ValidationHash),
      TypedEdgeHash(tip2.hash, EdgeHashType.TransactionHash),
      data = Some(TypedEdgeHash(tip1.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(oe)

    val roe = ResolvedObservationEdge(
      null.asInstanceOf[SignedObservationEdge],
      null.asInstanceOf[SignedObservationEdge],
      Some(cb)
    )

    val edge = Edge(oe, soe, roe)

    CheckpointEdge(edge)
  }

}
