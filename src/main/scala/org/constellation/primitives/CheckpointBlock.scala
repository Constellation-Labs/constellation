package org.constellation.primitives

import java.security.KeyPair

import constellation.signedObservationEdge
import org.constellation.DAO
import org.constellation.primitives.Schema._
import org.constellation.util.HashSignature


case class CheckpointBlock(
                            transactions: Seq[Transaction],
                            checkpoint: CheckpointEdge
                          ) {

  def storeSOE()(implicit dao: DAO): Unit = {
    dao.soeService.put(soeHash, SignedObservationEdgeCache(soe, resolved = true))
  }

  def calculateHeight()(implicit dao: DAO): Option[Height] = {

    val parents = parentSOEBaseHashes.map {
      dao.checkpointService.get
    }

    val maxHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map {_.get}
      val heights = parents2.map {_.height.map{_.max}}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.max + 1)
      }
    }

    val minHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map {_.get}
      val heights = parents2.map {_.height.map{_.min}}

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None else {
        Some(nonEmptyHeights.min + 1)
      }
    }

    val height = maxHeight.flatMap{ max =>
      minHeight.map{
        min =>
          Height(min, max)
      }
    }

    height

  }

  def transactionsValid: Boolean = transactions.nonEmpty && transactions.forall(_.valid)

  // TODO: Return checkpoint validation status for more info rather than just a boolean
  def simpleValidation()(implicit dao: DAO): Boolean = {

    val validation = CheckpointBlockValidatorNel.validateCheckpointBlock(
      CheckpointBlock(transactions, checkpoint)
    )

    if (validation.isValid) {
      dao.metrics.incrementMetric("checkpointValidationSuccess")
    } else {
      dao.metrics.incrementMetric("checkpointValidationFailure")
    }

    // TODO: Return Validation instead of Boolean
    validation.isValid
  }

  def uniqueSignatures: Boolean = signatures.groupBy(_.id).forall(_._2.size == 1)

  def signedBy(id: Id) : Boolean = witnessIds.contains(id)

  def hashSignaturesOf(id: Id) : Seq[HashSignature] = signatures.filter(_.id == id)

  def signatureConflict(other: CheckpointBlock): Boolean = {
    signatures.exists{s =>
      other.signatures.exists{ s2 =>
        s.signature != s2.signature && s.id == s2.id
      }
    }
  }

  def witnessIds: Seq[Id] = signatures.map{_.id}

  def signatures: Seq[HashSignature] = checkpoint.edge.signedObservationEdge.signatureBatch.signatures

  def baseHash: String = checkpoint.edge.baseHash

  def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

  // TODO: Optimize call, should store this value instead of recalculating every time.
  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def store(cache: CheckpointCacheData)(implicit dao: DAO): Unit = {
    /*
          transactions.foreach { rt =>
            rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
          }
    */
    // checkpoint.edge.storeCheckpointData(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved)
    dao.checkpointService.put(baseHash, cache)

  }

  def plus(keyPair: KeyPair): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(keyPair)))
  }

  def plus(hs: HashSignature): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.plus(hs)))
  }

  def plus(other: CheckpointBlock): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def +(other: CheckpointBlock): CheckpointBlock = {
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))
  }

  def parentSOE: Seq[TypedEdgeHash] = checkpoint.edge.parents
  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    parentSOEHashes.flatMap{dao.soeService.get}.map{_.signedObservationEdge.baseHash}

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

}


object CheckpointBlock {

  def createCheckpointBlockSOE(
                             transactions: Seq[Transaction],
                             tips: Seq[SignedObservationEdge],
                             messages: Seq[ChannelMessage] = Seq()
                           )(implicit keyPair: KeyPair): CheckpointBlock = {
    createCheckpointBlock(transactions, tips.map{t => TypedEdgeHash(t.hash, EdgeHashType.CheckpointHash)}, messages )
  }

  def createCheckpointBlock(
                             transactions: Seq[Transaction],
                             tips: Seq[TypedEdgeHash],
                             messages: Seq[ChannelMessage] = Seq()
                           )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData =
      CheckpointEdgeData(transactions.map { _.hash }.sorted, messages)

    val observationEdge = ObservationEdge(
      tips,
      TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlock(transactions, checkpointEdge)
  }

}