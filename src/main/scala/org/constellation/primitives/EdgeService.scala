package org.constellation.primitives

import java.security.KeyPair

import org.constellation.primitives.Schema._
import constellation._
import org.constellation.util.SignHelp

import scala.collection.concurrent.TrieMap

object EdgeService {

  case class CreateCheckpointEdgeResponse(checkpointEdge: SignedObservationEdge,
                                          transactionsUsed: Set[String],
                                          filteredValidationTips: Seq[TypedEdgeHash],
                                          updatedTransactionMemPoolThresholdMet: Set[String])

  def createCheckpointEdgeProposal(transactionMemPoolThresholdMet: Set[String],
                           minCheckpointFormationThreshold: Int,
                           validationTips: Seq[TypedEdgeHash])(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {
    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdge = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    val tips = validationTips.take(2)
    val filteredValidationTips = validationTips.filterNot(tips.contains)

    val oe = ObservationEdge(
      tips.head,
      tips(1),
      data = Some(TypedEdgeHash(checkpointEdge.hash, EdgeHashType.CheckpointDataHash))
    )

    val signedCheckpointEdge = SignHelp.signedObservationEdge(oe)(keyPair)

    CreateCheckpointEdgeResponse(signedCheckpointEdge, transactionsUsed, filteredValidationTips, updatedTransactionMemPoolThresholdMet)
  }

}
