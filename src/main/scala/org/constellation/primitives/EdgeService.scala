package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema._
import constellation._
import org.constellation.util.SignHelp

import scala.collection.concurrent.TrieMap

object EdgeService {

  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )

  def createCheckpointEdgeProposal(
                                    transactionMemPoolThresholdMet: Set[String],
                                    minCheckpointFormationThreshold: Int,
                                    validationTips: Seq[SignedObservationEdge],
                                  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    val tips = validationTips.take(2)
    val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.ValidationHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.ValidationHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val signedObservationEdge = SignHelp.signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(Edge(observationEdge, signedObservationEdge, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge, transactionsUsed, filteredValidationTips, updatedTransactionMemPoolThresholdMet)
  }

}
