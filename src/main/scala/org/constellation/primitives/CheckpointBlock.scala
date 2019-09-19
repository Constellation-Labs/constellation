package org.constellation.primitives

import java.security.KeyPair

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.effect.IO
import cats.implicits._
import constellation.signedObservationEdge
import org.constellation.DAO
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.Schema._
import org.constellation.util.{HashSignature, Metrics}

import scala.annotation.tailrec

abstract class CheckpointEdgeLike(val checkpoint: CheckpointEdge) {
  def baseHash: String = checkpoint.edge.baseHash

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    checkpoint.edge.parentHashes.flatMap { key =>
      dao.soeService.lookup(key).unsafeRunSync()
    }.map {
      _.signedObservationEdge.baseHash
    }

  def storeSOE()(implicit dao: DAO): Unit =
    dao.soeService.put(soeHash, SignedObservationEdgeCache(soe, resolved = true)).unsafeRunSync()

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def signatures: Seq[HashSignature] =
    checkpoint.edge.signedObservationEdge.signatureBatch.signatures
}

case class CheckpointBlockMetadata(
  transactionsMerkleRoot: Option[String],
  checkpointEdge: CheckpointEdge,
  messagesMerkleRoot: Option[String],
  notificationsMerkleRoot: Option[String],
  observationsMerkleRoot: Option[String]
) extends CheckpointEdgeLike(checkpointEdge)

case class CheckpointBlock(
  transactions: Seq[Transaction],
  checkpoint: CheckpointEdge,
  messages: Seq[ChannelMessage] = Seq(),
  notifications: Seq[PeerNotification] = Seq(),
  observations: Seq[Observation] = Seq()
) {

  def storeSOE()(implicit dao: DAO): IO[SignedObservationEdgeCache] =
    dao.soeService.put(soeHash, SignedObservationEdgeCache(soe, resolved = true))

  def calculateHeight()(implicit dao: DAO): Option[Height] = {

    val parents = parentSOEBaseHashes.map(dao.checkpointService.lookup(_).unsafeRunSync())

    dao.miscLogger.debug(s"[CB baseHash=$baseHash] Height calculation parents ${parentSOEBaseHashes()}")

    if (parents.exists(_.isEmpty)) {
      dao.miscLogger.error(s"[CB baseHash=$baseHash] Height calculation parent missing")
      dao.miscLogger.error(s"[CB baseHash=$baseHash] Height calculation parent are defined ${parents.map(_.isDefined)}")
      dao.metrics.incrementMetric("heightCalculationParentMissing")
    } else {
      dao.miscLogger.debug(s"[CB baseHash=$baseHash] Height calculation parent exists")
      dao.metrics.incrementMetric("heightCalculationParentsExist")
    }

    dao.metrics.incrementMetric("heightCalculationParentLength_" + parents.length)
    dao.miscLogger.debug(s"[CB baseHash=$baseHash] Height calculation parents length ${parents.length}")

    val maxHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map { _.get }
      val heights = parents2.map { _.height.map { _.max } }

      dao.miscLogger.debug(
        s"[CB baseHash=$baseHash] Height calculation MAX parents are defined: ${parents.forall(_.isDefined)}"
      )
      dao.miscLogger.debug(s"[CB baseHash=$baseHash] Height calculation MAX heights ${heights}")

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None
      else {
        Some(nonEmptyHeights.max + 1)
      }
    }

    val minHeight = if (parents.exists(_.isEmpty)) {
      None
    } else {

      val parents2 = parents.map { _.get }
      val heights = parents2.map { _.height.map { _.min } }

      dao.miscLogger.debug(
        s"[CB baseHash=$baseHash] Height calculation MIN parents are defined: ${parents.forall(_.isDefined)}"
      )
      dao.miscLogger.debug(s"[CB baseHash=$baseHash] Height calculation MIN heights ${heights}")

      val nonEmptyHeights = heights.flatten
      if (nonEmptyHeights.isEmpty) None
      else {
        Some(nonEmptyHeights.min + 1)
      }
    }

    val height = maxHeight.flatMap { max =>
      minHeight.map { min =>
        Height(min, max)
      }
    }

    height

  }

  def transactionsValid(implicit dao: DAO): Boolean = transactions.nonEmpty && transactions.forall(_.valid)

  def uniqueSignatures: Boolean = signatures.groupBy(_.id).forall(_._2.size == 1)

  def signedBy(id: Id): Boolean = witnessIds.contains(id)

  def hashSignaturesOf(id: Id): Seq[HashSignature] = signatures.filter(_.id == id)

  def signatureConflict(other: CheckpointBlock): Boolean =
    signatures.exists { s =>
      other.signatures.exists { s2 =>
        s.signature != s2.signature && s.id == s2.id
      }
    }

  def witnessIds: Seq[Id] = signatures.map { _.id }

  def signatures: Seq[HashSignature] =
    checkpoint.edge.signedObservationEdge.signatureBatch.signatures

  def baseHash: String = checkpoint.edge.baseHash

  def validSignatures: Boolean = signatures.forall(_.valid(baseHash))

  // TODO: Optimize call, should store this value instead of recalculating every time.

  def soeHash: String = checkpoint.edge.signedObservationEdge.hash

  def store(cache: CheckpointCache)(implicit dao: DAO): Unit = {
    /*
          transactions.foreach { rt =>
            rt.edge.store(db, Some(TransactionCacheData(rt, inDAG = inDAG, resolved = true)))
          }
     */
    // checkpoint.edge.storeCheckpointData(db, {prevCache: CheckpointCacheData => cache.plus(prevCache)}, cache, resolved)
    (cache.checkpointBlock.get.storeSOE() *> dao.checkpointService.memPool.put(baseHash, cache)).unsafeRunSync()
    dao.recentBlockTracker.put(cache)

  }

  def plus(keyPair: KeyPair): CheckpointBlock =
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignatureFrom(keyPair)))

  def plus(hs: HashSignature): CheckpointBlock =
    this.copy(checkpoint = checkpoint.copy(edge = checkpoint.edge.withSignature(hs)))

  def plus(other: CheckpointBlock): CheckpointBlock =
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))

  def +(other: CheckpointBlock): CheckpointBlock =
    this.copy(checkpoint = checkpoint.plus(other.checkpoint))

  def parentSOE: Seq[TypedEdgeHash] = checkpoint.edge.parents

  def parentSOEHashes: Seq[String] = checkpoint.edge.parentHashes

  def parentSOEBaseHashes()(implicit dao: DAO): Seq[String] =
    parentSOEHashes.flatMap { soeHash =>
      if (soeHash == Genesis.CoinBaseHash) {
        Seq()
      } else {
        val parent = dao.soeService.lookup(soeHash).unsafeRunSync()
        if (parent.isEmpty) {
          dao.miscLogger.debug(s"SOEHash $soeHash missing from soeService for cb: $baseHash")
          dao.metrics.incrementMetric("parentSOEServiceQueryFailed")
          // Temporary
          val parentDirect = checkpoint.edge.observationEdge.parents.find(_.hash == soeHash).flatMap { _.baseHash }
          if (parentDirect.isEmpty) {
            dao.metrics.incrementMetric("parentDirectTipReferenceMissing")
            //    throw new Exception("Missing parent direct reference")
          }
          parentDirect
        } else parent.map { _.signedObservationEdge.baseHash }
      }
    }

  def soe: SignedObservationEdge = checkpoint.edge.signedObservationEdge

}

object CheckpointBlock {

  def createCheckpointBlockSOE(
    transactions: Seq[Transaction],
    tips: Seq[SignedObservationEdge],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlock =
    createCheckpointBlock(transactions, tips.map { t =>
      TypedEdgeHash(t.hash, EdgeHashType.CheckpointHash)
    }, messages, peers)

  def createCheckpointBlock(
    transactions: Seq[Transaction],
    tips: Seq[TypedEdgeHash],
    messages: Seq[ChannelMessage] = Seq.empty,
    peers: Seq[PeerNotification] = Seq.empty,
    observations: Seq[Observation] = Seq.empty
  )(implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData =
      CheckpointEdgeData(transactions.map { _.hash }.sorted, messages.map {
        _.signedMessageData.hash
      })

    val observationEdge = ObservationEdge(
      tips.toList,
      TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash)
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, checkpointEdgeData)
    )

    CheckpointBlock(transactions, checkpointEdge, messages, peers, observations)
  }

}
