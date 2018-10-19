package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus.Consensus._
import org.constellation.consensus.EdgeProcessor.{HandleTransaction, _}
import org.constellation.consensus.Validation.TransactionValidationStatus
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.util.{HashSignature, HeartbeatSubscribe, ProductHash}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object EdgeProcessor {

  case class HandleTransaction(tx: Transaction)
  case class HandleCheckpoint(checkpointBlock: CheckpointBlock)
  case class HandleSignatureRequest(checkpointBlock: CheckpointBlock)

  val logger = Logger(s"EdgeProcessor")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)




  def handleCheckpointOld(cb: CheckpointBlock,
                       dao: DAO,
                       internalMessage: Boolean = false)(implicit executionContext: ExecutionContext): Unit = {

    if (!internalMessage) {
      dao.metricsManager ! IncrementMetric("checkpointMessages")
    } else {
      dao.metricsManager ! IncrementMetric("internalCheckpointMessages")
    }


    // TODO : Handle resolution issues.
    val potentialChildren = dao.resolveNotifierCallbacks.get(cb.soeHash)

    val cache = dao.dbActor.getCheckpointCacheData(cb.baseHash)

    def signFlow(): Unit = {
      // Mock
      // Check to see if we should add our signature to the CB

      // Note it's already met the threshold and we just haven't heard about it yet don't add signature
      // Not issue yet due to constraints of test.

      if (!dao.checkpointMemPoolThresholdMet.contains(cb.baseHash)) { // sanity check but shouldn't be required

        val cbPrime = updateCheckpointWithSelfSignatureEmit(cb, dao)

        // Add to memPool or update an existing hash with new signatures and check for signature threshold
        updateCheckpointMergeMemPool(cbPrime, dao)

        attemptFormCheckpointUpdateState(dao)
      }
    }

    def resolutionFlow(): Unit = {

      val resolved = Resolve.resolveCheckpoint(dao, cb)

      // TODO: Validate the checkpoint to see if any there are any duplicate transactions
      // Also need to register it with ldb ? To prevent duplicate processing ? Or after
      // If there are duplicate transactions, do a score calculation relative to the other one to determine which to preserve.
      // Potentially rolling back the other one.

      resolved.foreach { r =>

        if (r) {
          dao.metricsManager ! IncrementMetric("resolvedCheckpointMessages")

          val validatedTransactions = cb.transactions.map(Validation.validateTransaction(dao.dbActor, _))
          val validAccordingToCurrentState = validatedTransactions.forall(_.validByCurrentState)

            if (validAccordingToCurrentState) {
              signFlow()
            }
            else {
                val v = validatedTransactions.forall { s =>
                  cb.checkpoint.edge.parentHashes.forall { ancestorHash =>
                    s.validByAncestor(ancestorHash)
                  }
                }

                if (v) {
                  // resolveAncestryConflict(cb)
                }
              }
              // Check if parent tips are valid to merge.



          // TODO: Process children
          // Also need to store child references in parent.
          potentialChildren.foreach{
            _.foreach{ c =>
            //  Future(handleCheckpoint(c, dao, true))
            }
          }
          // Post resolution. onComplete

          // Need something to check if valid by ancestors

        } else {
          dao.metricsManager ! IncrementMetric("unresolvedCheckpointMessages")

        }
      }

    }
    /*

    def mainFlow(): Unit = {




        // Base hash not stored in DB, no possible signature conflict
        if (cache.isEmpty) {
          dao.metricsManager ! IncrementMetric("unknownCheckpointMessages")
          // resolutionFlow()
          // DEBUG
          signFlow()
        } else {

          val ca = cache.get

          if (ca.resolved) {
            if (ca.inDAG) {
              if (ca.checkpointBlock != cb) {
                // Data mismatch on base hash lookup, i.e. signature conflict
                // TODO: Conflict resolution
                //resolveConflict(cb, ca)
              } else {
                // Duplicate checkpoint message, no action required.
              }
            } else {
              // warn or store information about potential conflict
              // if block is not yet in DAG then it doesn't matter, can just store updated value or whatever
            }
          } else {

            // Data is stored but not resolved, potentially trigger resolution check to see if something failed?
            // Otherwise do nothing as the resolution is already in progress.

          }

          // if (ca.checkpointBlock

        }

    }

        mainFlow()
    */

    //  Resolve.resolveCheckpoint(dao, cb)
  }

  // TODO : Add checks on max number in mempool and max num signatures.

  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointWithSelfSignatureEmit(cb: CheckpointBlock, dao: DAO): CheckpointBlock = {
    val cbPrime = if (!cb.signatures.exists(_.publicKey == dao.keyPair.getPublic)) {
      // We haven't yet signed this CB

      val cb2Opt = (dao.cpSigner ? cb).mapTo[Option[CheckpointBlock]].get()
      cb2Opt.foreach { cb2 =>
        dao.metricsManager ! IncrementMetric("signaturesPerformed")
        // Send peers new signature
        dao.peerManager ! APIBroadcast(_.put(s"checkpoint/${cb.baseHash}", cb2))
        dao.metricsManager ! IncrementMetric("checkpointBroadcasts")
      }
      cb2Opt.getOrElse(cb)
    } else {
      // We have already signed this CB,
      cb
    }
    cbPrime
  }

  // TEMPORARY mock-up for pre-consensus integration mimics transactions
  def updateCheckpointMergeMemPool(cb: CheckpointBlock, dao: DAO) : Unit = {
    val cbPostUpdate = if (dao.checkpointMemPool.contains(cb.baseHash)) {
      // Merge signatures together


      val block = dao.checkpointMemPool(cb.baseHash)

      cb.signatures.foreach{
        hs =>
          if (block.signatures.exists{ s =>
            s.b58EncodedPublicKey == hs.b58EncodedPublicKey && s.signature != hs.signature
          }) {
            dao.metricsManager ! IncrementMetric("checkpointSignatureMemPoolMergeConflictOverlapDetected")
          }

      }

      val updated = block.plus(cb)
      // Update memPool with new signatures.
      dao.checkpointMemPool(cb.baseHash) = updated
      updated
    }
    else {
      dao.checkpointMemPool(cb.baseHash) = cb
      cb
    }

    dao.metricsManager ! IncrementMetric("checkpointNumSignatures_" + cbPostUpdate.signatures.size)
    dao.metricsManager ! UpdateMetric("checkpointSignatureThreshold", dao.minCBSignatureThreshold.toString)

    // TODO: Verify this is still valid before accepting. And/or
    // consider removing from memPool if there's a conflict on something else being accepted.
    // Check to see if we have enough signatures to include in CB
    if (cbPostUpdate.signatures.size >= dao.minCBSignatureThreshold) {
      // Set threshold as met
      dao.checkpointMemPoolThresholdMet(cb.baseHash) = cb -> 0
      dao.checkpointMemPool.remove(cb.baseHash)

      dao.metricsManager ! UpdateMetric("checkpointMemPool", dao.checkpointMemPool.size.toString)

      cb.parentSOEBaseHashes.foreach {
        h =>
          dao.checkpointMemPoolThresholdMet.get(h).foreach {
            case (block, numUses) =>

              // TODO: move to tips service
              // Update tips
              def doRemove(): Unit = {
                dao.checkpointMemPoolThresholdMet.remove(h)
                dao.metricsManager ! IncrementMetric("checkpointTipsRemoved")
              }

              if (dao.reuseTips) {
                if (numUses >= 2) {
                  doRemove()
                } else {
                  dao.checkpointMemPoolThresholdMet(h) = (block, numUses + 1)
                }
              } else {
                doRemove()
              }
          }


      }
      dao.metricsManager ! UpdateMetric("checkpointMemPoolThresholdMet", dao.checkpointMemPoolThresholdMet.size.toString)

      // Accept transactions
      cb.transactions.foreach { t =>
        dao.metricsManager ! IncrementMetric("transactionAccepted")
        t.store(
          dao.dbActor,
          TransactionCacheData(
            t,
            valid = true,
            inMemPool = false,
            inDAG = true,
            Map(cb.baseHash -> true),
            resolved = true,
            cbBaseHash = Some(cb.baseHash)
          ))
        t.ledgerApply(dao.dbActor)
      }
      dao.metricsManager ! IncrementMetric("checkpointAccepted")
      cb.store(
        dao.dbActor,
        CheckpointCacheData(
          cb,
          inDAG = true,
          resolved = true
        ),
        resolved = true
      )
    }
  }

  def acceptCheckpoint(cb: CheckpointBlock)(implicit dao: DAO): Unit = {
    // Accept transactions
    cb.transactions.foreach { t =>
      dao.metricsManager ! IncrementMetric("transactionAccepted")
      t.store(
        dao.dbActor,
        TransactionCacheData(
          t,
          valid = true,
          inMemPool = false,
          inDAG = true,
          Map(cb.baseHash -> true),
          resolved = true,
          cbBaseHash = Some(cb.baseHash)
        ))
      t.ledgerApply(dao.dbActor)
    }
    dao.metricsManager ! IncrementMetric("checkpointAccepted")
    cb.store(
      dao.dbActor,
      CheckpointCacheData(
        cb,
        inDAG = true,
        resolved = true
      ),
      resolved = true
    )
  }

  def attemptFormCheckpointUpdateState(dao: DAO): Option[CheckpointBlock] = {

    // TODO: Send a DBUpdate to modify tip data to include newly formed CB as a 'child', but only after acceptance
    if (dao.canCreateCheckpoint) {
      // Form new checkpoint block.

      // TODO : Validate this batch doesn't have a double spend, if it does,
      // just drop all conflicting. Shouldn't be necessary since memPool is already validated
      // relative to current state but it can't hurt

      val tips = Random.shuffle(dao.checkpointMemPoolThresholdMet.toSeq).take(2)

      val tipSOE = tips.map {_._2._1.checkpoint.edge.signedObservationEdge}

      val transactions = Random.shuffle(dao.transactionMemPool).take(dao.minCheckpointFormationThreshold)
      dao.transactionMemPool = dao.transactionMemPool.filterNot(transactions.contains)

      val checkpointBlock = createCheckpointBlock(transactions, tipSOE)(dao.keyPair)
      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      val cbBaseHash = checkpointBlock.baseHash

      dao.checkpointMemPool(cbBaseHash) = checkpointBlock
      dao.metricsManager ! UpdateMetric("checkpointMemPool", dao.checkpointMemPool.size.toString)

      // Temporary bypass to consensus for mock
      // Send all data (even if this is redundant.)
      dao.peerManager ! APIBroadcast(_.put(s"checkpoint/$cbBaseHash", checkpointBlock))

      Some(checkpointBlock)
    } else None
  }

  /**
    * Main transaction processing cell
    * This is triggered upon external receipt of a transaction. Assume that the transaction being processed
    * came from a peer, not an internal operation.
    * @param tx : Transaction with all data
    * @param dao : Data access object for referencing memPool and other actors
    * @param executionContext : Threadpool to execute transaction processing against. Should be separate
    *                         from other pools for processing different operations.
    */
  def handleTransaction(
                         tx: Transaction, memPool: MemPool, skipValidation: Boolean = true
                       )(implicit executionContext: ExecutionContext, dao: DAO): MemPool = {

    // TODO: Store TX in DB immediately rather than waiting
    // ^ Requires TX hash partitioning or conflict stuff later + gossiping tx's to wider reach

    dao.metricsManager ! IncrementMetric("transactionMessagesReceived")
    // Validate transaction TODO : This can be more efficient, calls get repeated several times
    // in event where a new signature is being made by another peer it's most likely still valid, should
    // cache the results of this somewhere.

// TODO: Move memPool update logic to actor and send response back to EdgeProcessor
    if (!memPool.transactions.contains(tx)) {
      val pool = memPool.copy(transactions = memPool.transactions + tx)
      dao.metricsManager ! UpdateMetric("transactionMemPool", pool.transactions.size.toString)
      dao.metricsManager ! IncrementMetric("transactionValidMessages")
      val updated = formCheckpoint(pool) // TODO: Send to checkpoint formation actor instead
      updated
    } else {
      dao.metricsManager ! IncrementMetric("transactionValidMemPoolDuplicateMessages")
      memPool
    }

    /*    val finished = Validation.validateTransaction(dao.dbActor, tx)



        finished match {
          // TODO : Increment metrics here for each case
          case t : TransactionValidationStatus if t.validByCurrentStateMemPool =>


            // TODO : Store something here for status queries. Make sure it doesn't cause a conflict

          case t : TransactionValidationStatus =>

            // TODO : Add info somewhere so node can find out transaction was invalid on a callback
            reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus)
            memPool
        }

        */

  }

  def reportInvalidTransaction(dao: DAO, t: TransactionValidationStatus): Unit = {
    dao.metricsManager ! IncrementMetric("invalidTransactions")
    if (t.isDuplicateHash) {
      dao.metricsManager ! IncrementMetric("hashDuplicateTransactions")
    }
    if (!t.sufficientBalance) {
      dao.metricsManager ! IncrementMetric("insufficientBalanceTransactions")
    }
  }

  def triggerCheckpointBlocking(dao: DAO,
                                tx: Transaction)(implicit timeout: Timeout, executionContext: ExecutionContext): Unit = {
    if (dao.canCreateCheckpoint) {

      println(s"starting checkpoint blocking")

      val checkpointBlock = attemptFormCheckpointUpdateState(dao).get

      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      val cbBaseHash = checkpointBlock.baseHash
      dao.checkpointMemPool(cbBaseHash) = checkpointBlock

      // TODO: should be subset
      val facilitators = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().keySet

      // TODO: what is the round hash based on?
      // what are thresholds and checkpoint selection

      val obe = checkpointBlock.checkpoint.edge.observationEdge

      val roundHash = RoundHash(obe.left.hash + obe.right.hash)

      // Start check pointing consensus round
      /*      dao.consensus ! InitializeConsensusRound(facilitators, roundHash, (result) => {
              println(s"consensus round complete result roundHash = $roundHash, result = $result")
              EdgeProcessor.handleCheckpoint(result.checkpointBlock, dao)
            }, CheckpointVote(checkpointBlock))*/
      dao.consensus ! ConsensusVote(dao.id, CheckpointVote(checkpointBlock), roundHash)

    }
  }

  case class CreateCheckpointEdgeResponse(
                                           checkpointEdge: CheckpointEdge,
                                           transactionsUsed: Set[String],
                                           // filteredValidationTips: Seq[SignedObservationEdge],
                                           updatedTransactionMemPoolThresholdMet: Set[String]
                                         )


  def createCheckpointBlock(transactions: Seq[Transaction], tips: Seq[SignedObservationEdge])
                           (implicit keyPair: KeyPair): CheckpointBlock = {

    val checkpointEdgeData = CheckpointEdgeData(transactions.map{_.hash}.sorted)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(
      Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData)))
    )

    CheckpointBlock(transactions, checkpointEdge)
  }

  def createCheckpointEdgeProposal(
                                    transactionMemPoolThresholdMet: Set[String],
                                    minCheckpointFormationThreshold: Int,
                                    tips: Seq[SignedObservationEdge],
                                  )(implicit keyPair: KeyPair): CreateCheckpointEdgeResponse = {

    val transactionsUsed = transactionMemPoolThresholdMet.take(minCheckpointFormationThreshold)
    val updatedTransactionMemPoolThresholdMet = transactionMemPoolThresholdMet -- transactionsUsed

    val checkpointEdgeData = CheckpointEdgeData(transactionsUsed.toSeq.sorted)

    //val tips = validationTips.take(2)
    //val filteredValidationTips = validationTips.filterNot(tips.contains)

    val observationEdge = ObservationEdge(
      TypedEdgeHash(tips.head.hash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(tips(1).hash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(checkpointEdgeData.hash, EdgeHashType.CheckpointDataHash))
    )

    val soe = signedObservationEdge(observationEdge)(keyPair)

    val checkpointEdge = CheckpointEdge(Edge(observationEdge, soe, ResolvedObservationEdge(tips.head, tips(1), Some(checkpointEdgeData))))

    CreateCheckpointEdgeResponse(checkpointEdge, transactionsUsed,
      //filteredValidationTips,
      updatedTransactionMemPoolThresholdMet)
  }
  // TODO: Send facilitator selection data (i.e. criteria) as well for verification

  case class SignatureRequest(checkpointBlock: CheckpointBlock, facilitators: Set[Id])
  case class SignatureResponse(checkpointBlock: CheckpointBlock, facilitators: Set[Id])
  case class FinishedCheckpoint(checkpointBlock: CheckpointBlock, facilitators: Set[Id])

  // TODO: Move to checkpoint formation actor
  def formCheckpoint(memPool: MemPool)(implicit dao: DAO): MemPool = {

    def canCreateCheckpoint: Boolean = {
      memPool.transactions.size >= dao.minCheckpointFormationThreshold &&
        memPool.thresholdMetCheckpoints.size >= 2 && memPool.facilitators.nonEmpty
    }

    val postCreationMemPool = if (!canCreateCheckpoint) memPool else {

      val tips = Random.shuffle(memPool.thresholdMetCheckpoints.toSeq).take(2)

      val tipSOE = tips.map {_._2.checkpointBlock.checkpoint.edge.signedObservationEdge}.sortBy(_.hash)

      val mergedTipHash = tipSOE.map{_.hash}.mkString("")

      val totalNumFacil = memPool.facilitators.size
      // TODO: Use XOR distance instead as it handles peer data mismatch cases better
      val facilitatorIndex = (BigInt(mergedTipHash, 16) % totalNumFacil).toInt
      val sortedFacils = memPool.facilitators.toSeq.sortBy(_._1.encodedId.b58Encoded)
      val selectedFacils = Seq.tabulate(2){i => (i + facilitatorIndex) % totalNumFacil }.map{sortedFacils(_)}

      val transactions = Random.shuffle(memPool.transactions).take(dao.minCheckpointFormationThreshold)

      val checkpointBlock = createCheckpointBlock(transactions.toSeq, tipSOE)(dao.keyPair)
      dao.metricsManager ! IncrementMetric("checkpointBlocksCreated")

      val newMemPool = memPool.copy(transactions = memPool.transactions.diff(transactions)).plus(checkpointBlock)

      // Temporary bypass to consensus for mock
      // Send all data (even if this is redundant.)
      val finalFacilitators = selectedFacils.map {_._1}.toSet
      dao.peerManager ! APIBroadcast(
        _.post(s"request/signature", SignatureRequest(checkpointBlock, finalFacilitators + dao.id)),
        peerSubset = finalFacilitators
      )

      newMemPool
    }

    postCreationMemPool

  }

  // Temporary for testing join/leave logic.
  def handleSignatureRequest(sr: SignatureRequest)(implicit kp: KeyPair, dao: DAO): Unit = {
    if (sr.facilitators.contains(dao.id)) {
      val replyTo = sr.checkpointBlock.witnessIds.head
      val updated = sr.checkpointBlock.plus(kp)
      dao.peerManager ! APIBroadcast(
        _.post(s"response/signature", SignatureResponse(updated, sr.facilitators)),
        peerSubset = Set(replyTo)
      )
    }
  }

  def handleSignatureResponse(sr: SignatureResponse, memPool: MemPool)(implicit kp: KeyPair, dao: DAO): (CheckpointBlock, MemPool) = {

    val cb = sr.checkpointBlock

    val (cbAfterProcessing, updatedMemPool) = memPool.checkpoints.get(cb.baseHash).map{
      mcb =>

        if (mcb.signatureConflict(cb)) {
          dao.metricsManager ! IncrementMetric("checkpointSignatureMergeConflictDetectedERROR")
        }

        val updated1 = mcb.plus(cb)
        val updated = updated1 //if (updated1.signedBy(dao.id)) updated1 else updated1.plus(kp)
        updated -> memPool.plus(updated, facilitatorCount = Some(sr.facilitators.size))
    }.getOrElse{

      // Not stored anywhere, first observance
      dao.metricsManager ! IncrementMetric("checkpointFirstObserved")

      val updated = cb // if (cb.signedBy(dao.id)) cb else cb.plus(kp)
      updated -> memPool.plus(updated, facilitatorCount = Some(sr.facilitators.size))

    }

    if (updatedMemPool.thresholdMetCheckpoints.contains(cbAfterProcessing.baseHash)) {
      dao.peerManager ! APIBroadcast(_.post(s"finished/checkpoint", FinishedCheckpoint(cbAfterProcessing, sr.facilitators)))
    }


    // TODO: Return actual newly formed checkpoint as optional metadata to attach to a proper response
    // To the sender() with all this info including the cache lookup and validation status etc. and what updates happened

    val postCreationMemPool = formCheckpoint(updatedMemPool)

    dao.metricsManager ! UpdateMetric("checkpointMemPool", postCreationMemPool.checkpoints.size.toString)
    dao.metricsManager ! UpdateMetric("checkpointMemPoolThresholdMet", postCreationMemPool.thresholdMetCheckpoints.size.toString)
    dao.metricsManager ! UpdateMetric("transactionMemPool", postCreationMemPool.transactions.size.toString)

    cbAfterProcessing -> postCreationMemPool


  }

  def handleCheckpoint(cb: CheckpointBlock, memPool: MemPool)(implicit kp: KeyPair, dao: DAO): (CheckpointBlock, MemPool) = {
    val (cbAfterProcessing, updatedMemPool) = if (!cb.uniqueSignatures) {
      dao.metricsManager ! IncrementMetric("checkpointUniqueSignaturesFailure")
      cb -> memPool
    } else {

      val cache = dao.dbActor.getCheckpointCacheData(cb.baseHash)

      cache.map { c =>
        dao.metricsManager ! IncrementMetric("checkpointCacheFound")
        // Do nothing, we've already signed and accepted it
        c.checkpointBlock -> memPool
      }.getOrElse{

        dao.metricsManager ! IncrementMetric("checkpointCacheEmpty")

        // Check if stored as threshold met
        memPool.thresholdMetCheckpoints.get(cb.baseHash).map{
          tmcb =>
            dao.metricsManager ! IncrementMetric("checkpointThresholdMetLookupFound")
            // Do nothing, threshold already met
            tmcb.checkpointBlock -> memPool
        }.getOrElse{
          // Check if in memPool now
          memPool.checkpoints.get(cb.baseHash).map{
            mcb =>

              if (mcb.signatureConflict(cb)) {
                dao.metricsManager ! IncrementMetric("checkpointSignatureMergeConflictDetectedERROR")
              }

              val updated1 = mcb.plus(cb)
              val updated = if (updated1.signedBy(dao.id)) updated1 else updated1.plus(kp)
              updated -> memPool.plus(updated)
          }.getOrElse{

            // Not stored anywhere, first observance
            dao.metricsManager ! IncrementMetric("checkpointFirstObserved")

            val updated = if (cb.signedBy(dao.id)) cb else cb.plus(kp)
            updated -> memPool.plus(updated)

          }
        }
      }
    }



    if (cbAfterProcessing != cb) {
      dao.peerManager ! APIBroadcast(_.put(s"checkpoint/${cbAfterProcessing.baseHash}", cbAfterProcessing))
    }


    // TODO: Return actual newly formed checkpoint as optional metadata to attach to a proper response
    // To the sender() with all this info including the cache lookup and validation status etc. and what updates happened

    val postCreationMemPool = formCheckpoint(updatedMemPool)

    dao.metricsManager ! UpdateMetric("checkpointMemPool", postCreationMemPool.checkpoints.size.toString)
    dao.metricsManager ! UpdateMetric("checkpointMemPoolThresholdMet", postCreationMemPool.thresholdMetCheckpoints.size.toString)
    dao.metricsManager ! UpdateMetric("transactionMemPool", postCreationMemPool.transactions.size.toString)

    cbAfterProcessing -> postCreationMemPool

  }

}

case class TipData(checkpointBlock: CheckpointBlock, numUses: Int)

case class SnapshotInfo(
                         snapshot: Snapshot,
                         acceptedCBSinceSnapshot: Seq[String] = Seq()
                       )

case class MemPool(
                    transactions: Set[Transaction] = Set(),
                    checkpoints: Map[String, CheckpointBlock] = Map(),
                    thresholdMetCheckpoints: Map[String, TipData] = Map(),
                    facilitators: Map[Id, PeerData] = Map(),
                    heartBeatRound: Int = 0,
                    snapshot: Snapshot = Snapshot("", Seq()),
                    acceptedCBSinceSnapshot: Seq[String] = Seq()
                  ) {
  def plus(checkpointBlock: CheckpointBlock, facilitatorCount: Option[Int] = None)(implicit dao: DAO): MemPool = {

    // TODO: Remove option and use one or the other
    def finished: Boolean = facilitatorCount.map{
      checkpointBlock.signatures.size == _
    }.getOrElse(checkpointBlock.signatures.size >= dao.minCBSignatureThreshold)

    if (finished) {

      acceptCheckpoint(checkpointBlock)

      def reuseTips: Boolean = thresholdMetCheckpoints.size < dao.maxWidth

      val keysToRemove = checkpointBlock.parentSOEBaseHashes.flatMap {
        h =>
          thresholdMetCheckpoints.get(h).flatMap {
            case TipData(block, numUses) =>

              def doRemove(): Option[String] = {
                dao.metricsManager ! IncrementMetric("checkpointTipsRemoved")
                Some(block.baseHash)
              }

              if (reuseTips) {
                if (numUses >= 2) {
                  doRemove()
                } else {
                  None
                }
              } else {
                doRemove()
              }
          }
      }

      val keysToUpdate = checkpointBlock.parentSOEBaseHashes.flatMap {
        h =>
          thresholdMetCheckpoints.get(h).flatMap {
            case TipData(block, numUses) =>

              def doUpdate(): Option[(String, TipData)] = {
                dao.metricsManager ! IncrementMetric("checkpointTipsIncremented")
                Some(block.baseHash -> TipData(block, numUses + 1))
              }

              if (reuseTips && numUses <= 2) {
                doUpdate()
              } else None
          }
      }.toMap


      this.copy(
        checkpoints = checkpoints - checkpointBlock.baseHash,
        thresholdMetCheckpoints = thresholdMetCheckpoints +
          (checkpointBlock.baseHash -> TipData(checkpointBlock, 0)) ++
          keysToUpdate --
          keysToRemove,
        acceptedCBSinceSnapshot = acceptedCBSinceSnapshot :+ checkpointBlock.baseHash
      )
    } else {
      this.copy(
        checkpoints = checkpoints + (checkpointBlock.baseHash -> checkpointBlock)
      )
    }
  }
}

case object GetMemPool

case class Snapshot(lastSnapshot: String, checkpointBlocks: Seq[String]) extends ProductHash

case class DownloadComplete(latestSnapshot: Snapshot)

class EdgeProcessor(dao: DAO)
                   (implicit timeout: Timeout, executionContext: ExecutionContext) extends Actor with ActorLogging {

  implicit val sys: ActorSystem = context.system
  implicit val kp: KeyPair = dao.keyPair
  implicit val _dao: DAO = dao

  dao.heartbeatActor ! HeartbeatSubscribe

  private val snapshotZero = Snapshot("", Seq()).hash

  def receive: Receive = active(MemPool())

  def active(memPool: MemPool): Receive = {

    case DownloadComplete(latestSnapshot) =>

      dao.nodeState = NodeState.Ready
      dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)
      dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, NodeState.Ready)))

      context become active(memPool.copy(
        snapshot = latestSnapshot,
        acceptedCBSinceSnapshot = memPool.acceptedCBSinceSnapshot.filterNot(latestSnapshot.checkpointBlocks.contains)
      ))


    case InternalHeartbeat =>

      val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq
      val facilitators = peerIds.filter{case (_, pd) =>
        pd.timeAdded < (System.currentTimeMillis() - 30*1000) && pd.nodeState == NodeState.Ready
      }.toMap

      val snapshotUpdatedMemPool = if (
        memPool.heartBeatRound % dao.snapshotInterval == 0 &&
        memPool.acceptedCBSinceSnapshot.nonEmpty && dao.nodeState == NodeState.Ready
      ) {
        val snapshot = Snapshot(memPool.snapshot.hash, memPool.acceptedCBSinceSnapshot)
        dao.dbActor.putSnapshot(snapshot.hash, snapshot)
        if (snapshot.lastSnapshot != snapshotZero) {
          val lastSnapshotVerification = dao.dbActor.getSnapshot(snapshot.lastSnapshot)
          if (lastSnapshotVerification.isEmpty) {
            dao.metricsManager ! IncrementMetric("snapshotVerificationFailed")
          } else {
            dao.metricsManager ! IncrementMetric("snapshotVerificationCount")
          }
        }
        dao.metricsManager ! IncrementMetric("snapshotCount")
        memPool.copy(snapshot = snapshot, acceptedCBSinceSnapshot = Seq())
      } else memPool
      context become active(snapshotUpdatedMemPool.copy(facilitators = facilitators, heartBeatRound = memPool.heartBeatRound + 1))

    case GetMemPool =>
      sender() ! memPool

    case go: GenesisObservation =>

      // Dumb way to set these as active tips, won't pass a double validation but no big deal.
      dao.acceptGenesis(go)
      context become active(go.initialMemPool)

    case HandleTransaction(transaction) =>

      context become active(handleTransaction(transaction, memPool))

    case sr: SignatureRequest =>

      handleSignatureRequest(sr)

    case sr: SignatureResponse =>

      val (_, postCreationMemPool) = handleSignatureResponse(sr, memPool)
      context become active(postCreationMemPool)

    case fc: FinishedCheckpoint =>

      if (fc.checkpointBlock.signatures.size == fc.facilitators.size) {
        context become active(memPool.plus(fc.checkpointBlock, Some(fc.facilitators.size)))
      }


    case HandleCheckpoint(cb) =>

      val (cbAfterProcessing, postCreationMemPool) = handleCheckpoint(cb, memPool)
      sender() ! cbAfterProcessing
      context become active(postCreationMemPool)

    case ConsensusRoundResult(checkpointBlock, roundHash: RoundHash[Checkpoint]) =>
      log.debug(s"handle checkpointBlock = $checkpointBlock")

     // handleCheckpoint(checkpointBlock, dao)

  }

}


// Deprecated but maybe use later
@deprecated class CheckpointUniqueSigner(dao: DAO)
                            (implicit timeout: Timeout, executionContext: ExecutionContext) extends Actor with ActorLogging {

  private val id: Id = dao.id
  private val kp: KeyPair = dao.keyPair


  import com.twitter.storehaus.cache._
  val cache: LRUCache[String, HashSignature] = LRUCache[String, HashSignature](20000)

  def receive: Receive = {

    case cb: CheckpointBlock =>

      val signatures = cb.hashSignaturesOf(id)

      val ret = if (!cb.uniqueSignatures) {

        dao.metricsManager ! IncrementMetric("checkpointSignatureDuplicateDetected")

        if (signatures.size > 1) {
          dao.metricsManager ! IncrementMetric("checkpointDuplicateSelfSignatureDetectedERROR")
        }

        None
      } else {

        val sigOpt = cache.get(cb.baseHash)
        // val memPool = dao.checkpointMemPool.get(cb.baseHash)
        // val dbCB = (dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointBlock]].get()

        if (cb.signedBy(id)) {

          val signatures = cb.hashSignaturesOf(id)

          if (sigOpt.isEmpty) {
            dao.metricsManager ! IncrementMetric("checkpointSignatureMissingFromCache")
            cache.put(cb.baseHash, signatures.head)
          }

          if (sigOpt.exists(hs => !signatures.contains(hs))) {
            dao.metricsManager ! IncrementMetric("checkpointDuplicateSelfCacheSignatureDetectedERROR")
            None
          } else Some(cb)

          // TODO: If mempool or DB is inconsistent need to update them, probably not necessarily

        } else {

          val cb2 = sigOpt.map{cb.plus}.getOrElse(cb.plus(kp))
          Some(cb2)
        }
      }

      sender() ! ret
  }

}




// Deprecated but maybe re-use later
@deprecated class CheckpointMemPoolVerifier(dao: DAO)
                               (implicit timeout: Timeout, executionContext: ExecutionContext) extends Actor with ActorLogging {

  // dao.heartbeatActor ! HeartbeatSubscribe

  private var lastTime = System.currentTimeMillis()

  def receive: Receive = {

    case InternalHeartbeat =>

      if (System.currentTimeMillis() > (lastTime + 10000) && dao.nodeState == NodeState.Ready) {

        val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq.map{_._1}

        dao.checkpointMemPool.take(20).foreach{
          case (_, checkpointBlock) =>

            if (!checkpointBlock.witnessIds.contains(dao.id)) {
              Future {
                val cbPrime = updateCheckpointWithSelfSignatureEmit(checkpointBlock, dao)

                // Add to memPool or update an existing hash with new signatures and check for signature threshold
                updateCheckpointMergeMemPool(cbPrime, dao)

                attemptFormCheckpointUpdateState(dao)
              }(dao.edgeExecutionContext)
            } else {
              val missing = peerIds.filterNot(checkpointBlock.witnessIds.contains)
              dao.peerManager ! APIBroadcast(_.post(s"request/signature", checkpointBlock).foreach{
                z =>
                  val cbP = z.body.x[CheckpointBlock]
                  val cbPrime = updateCheckpointWithSelfSignatureEmit(cbP, dao)

                  // Add to memPool or update an existing hash with new signatures and check for signature threshold
                  updateCheckpointMergeMemPool(cbPrime, dao)

                  attemptFormCheckpointUpdateState(dao)
              }(dao.edgeExecutionContext), peerSubset = missing.toSet)
            }


        }

        lastTime = System.currentTimeMillis()
      }

  }

}
