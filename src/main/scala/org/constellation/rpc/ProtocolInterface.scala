package org.constellation.rpc

import java.security.PublicKey

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Receiver
import org.constellation.blockchain._
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{GetBalance, GetId, Id}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Try}

object ProtocolInterface {

  case class AccountData(var balance: Long,
                         unsignedTxs: mutable.HashMap[String, Tx] = mutable.HashMap[String, Tx](),
                         metaData: List[BlockData] = Nil, //TODO: we'll need to figure out some sort of buffering
                         var sequenceNumber: Long = 0L
                        )

  case class GetLatestBlock()

  case class GetChain()

  case class FullChain(blockChain: Seq[Block])

  case class ResponseBlock(block: Block)

  case class Balance(balance: Long)

  case class TXBufferRequest()

  case class BlockProposal(block: Block, senderId: PublicKey)

  case class BlockProposalRequest()

  case class SetMaster()

  case class GetAccountDetails(key: PublicKey)

  /**
    * Stubbed, this will use the checkpoint block and validSignature
    *
    * @param tx
    * @return
    */
  def validBlockData(tx: BlockData): Boolean = true

  def validCheckpointBlock(cp: Block): Boolean = true

  /**
    * This will require some logic in the DAG object, which passes the block and proposed delegate list to the generating
    * function (coalgebra class)
    *
    * @return Boolean is this currently Node a delegate
    */
  def isDelegate: Boolean = true

  def validSignature(tx1: Tx, tx2: Tx): Boolean = true

  def genesisFromTX(transaction: Transaction): Block = {
    Block(
      parentHash = "2/8/18 The Problems With Blockchain and The Company With Solutions - " +
        "https://www.influencive.com/problems-blockchain-company-solutions",
      height = 0L,
      transactions = Seq(transaction)
    )
  }

}

import ProtocolInterface._
import java.util.concurrent._
import constellation._

trait ProtocolInterface {
  this: PeerToPeer with Receiver =>

  import ProtocolInterface._

  val logger = Logger("ProtocolInterface")

  val buffer: ListBuffer[Transaction] = new ListBuffer()
  val blockBuffer: ListBuffer[Block] = new ListBuffer()
  val chainCache: mutable.HashMap[PublicKey, AccountData] = mutable.HashMap()
  val signatureBuffer = new ListBuffer[BlockData]()

  val chain = new DAG()
  import chain._
  val publicKey: PublicKey
  val name: String = self.path.name

  var proposedBlock: Option[Block] = None
  var isMaster = false

  val ex = new ScheduledThreadPoolExecutor(10)
  private val bufferTask = new Runnable { def run(): Unit = {
    //   logger.info(s"Checking transaction buffer of length ${buffer.length} on ${self.path.name}")
    val res = broadcastAsk(TXBufferRequest).map {_.mapTo[ListBuffer[Transaction]].get()}
    val bufferAgreement = (res ++ Seq(buffer)).distinct.length == 1
    //    logger.info(s"BufferAgreement: $bufferAgreement on $name: num results: ${res.length}")
    //   logger.info(s"ProposedBlock $name ${proposedBlock.nonEmpty}")

    // logger.info(s"isMaster: $isMaster")
    if (bufferAgreement && proposedBlock.isEmpty && isMaster) {
      val b = Block(globalChain.last.hash, globalChain.size, transactions = buffer.toArray.toSeq)
      //    logger.info(s"Proposing block $b")
      proposedBlock = Some(b)
      broadcast(b)
    }

    if (proposedBlock.nonEmpty) {
      val blocks = broadcastAsk(BlockProposalRequest).map {_.mapTo[Option[Block]].get()}
      val distinctBlocks = (blocks.flatten ++ Seq(proposedBlock.get)).distinct
      val distinctNumBlocks = distinctBlocks.length
      val blockAgreement = blocks.forall(_.nonEmpty) && distinctNumBlocks == 1
      //  logger.info(s"Block proposal request on $name agreement? $blockAgreement height: ${proposedBlock.get.height}" +
      //   s" numEmpty: ${blocks.count(_.isEmpty)} distinctNumBlocks: $distinctNumBlocks")
      //  logger.info(s"Distinct blocks: $distinctBlocks")
      if (blockAgreement && isMaster) {
        val proposal = BlockProposal(proposedBlock.get, publicKey)
        broadcast(proposal)
        self ! proposal
      }
    }

  } }

  val bufferMonitor: ScheduledFuture[_] = ex.scheduleAtFixedRate(bufferTask, 1, 2, TimeUnit.SECONDS)

  receiver {

    case GetAccountDetails(key) =>
      sender() ! chainCache.get(key)

    case SetMaster =>
      logger.info("Set master true")
      isMaster = true

    case BlockProposal(block, _) =>
      //    logger.info(s"Accepting block ${block.height} with ${block.transactions.length} transactions on $name")
      if (!globalChain.contains(block)) {
        proposedBlock = None
        buffer.clear()
        addBlock(block)
      }

    case BlockProposalRequest =>
      sender() ! proposedBlock

    case b: Block =>
      //     logger.info(s"$name received block: ${b.height}")
      proposedBlock = Some(b)

    case TXBufferRequest =>
      sender() ! buffer
    case transaction: Transaction =>
      //    logger.info(s"received transaction from ${sender()}")
      if (globalChain.isEmpty && chainCache.isEmpty && buffer.isEmpty) {
        // Genesis block -- there's other ways to do this more properly but this is convenient for now
        val gen = genesisFromTX(transaction)
        addBlock(gen)
      } else {
        if (!buffer.contains(transaction)) {
          buffer += transaction
          broadcast(transaction) // Consider broadcasting even if we're aware. for now just making it safer
        }
      }
    // If we send another node a transaction it will create a loop where it continually rebroadcasts.
    // sender() ! transaction

    case GetLatestBlock =>
      logger.info(s"received GetLatestBlock request from ${sender()}")
      sender() ! chain.globalChain.headOption

    case GetChain =>
      //    logger.info(s"received GetChain request from ${sender()}")
      //   logger.info(s"chain is ${chain.globalChain}")
      sender() ! FullChain(chain.globalChain)

    case GetId =>
      logger.info(s"received GetId request ${sender()}")
      sender() ! Id(publicKey)

    /*
    This should eventually query up the tiers. Would be part of a rest service for quick tx signing (ensuring both sigs in one block)
     */
    case GetBalance(account) =>
      val balance = chainCache.get(account).map(_.balance).getOrElse(0L)
      logger.info(s"received GetBalance request, balance is: $balance")
      sender() ! Balance(balance)

  //  case checkpointBlock: Block => // This should be coupled with a state transition and within a separate Consensus process.
   //   logger.info(s"received CheckpointBlock request ${sender()}")
   //   if (validCheckpointBlock(checkpointBlock))
   //     addBlock(checkpointBlock)
   //   sender() ! checkpointBlock

    case fullChain: FullChain =>
      val newBlocks = fullChain.blockChain.diff(chain.globalChain)
      logger.info(s"received fullChain from ${sender()} \n diff is $newBlocks")
      if (newBlocks.nonEmpty && newBlocks.forall(validCheckpointBlock)){
        newBlocks.foreach(addBlock)
        sender() ! FullChain(chain.globalChain)
      }
  }

  def cacheKeyUpdate(
                      key: PublicKey, amount: Long
                    ): Unit = {
    val accountData = chainCache.get(key)
    accountData.foreach{ ad => ad.balance += amount; ad.sequenceNumber += 1L}
    if (accountData.isEmpty) { chainCache(key) = AccountData(amount)}
  }

  /**
    * Append block to the globalChain
    *
    * @param block CheckpointBlock to be added
    * @return Unit
    */
  def addBlock(block: Block): Unit = {
    //  logger.info(s"block $block Added")
    chain.globalChain.append(block)
    // TODO: Use a proper update ledger state func below after full integrations. This is just for MVP demo
    block.transactions.foreach{ tx =>
      cacheKeyUpdate(tx.senderPubKey, -1*tx.amount)
      cacheKeyUpdate(tx.counterPartyPubKey, tx.amount)
    }

  }

  /**
    * We're prob going to want a buffering service to handle validation/updates to chain cache, this is for maintaining
    * balances on the ledger
    *
    * @param buffer ListBuffer[Tx] all Tx's within the buffer that will now be used to update chain state
    */
  def updateLedgerState(buffer: ListBuffer[Tx]): Unit = {
    val validatedBuffer: mutable.Seq[Tx] = buffer.filter(validBlockData)

    validatedBuffer.foreach { tx: Tx =>
      val counterParty = chainCache.get(tx.counterPartyPubKey)
      val initialTransaction = counterParty.flatMap(_.unsignedTxs.get(tx.hash))

      initialTransaction match {
        case Some(initialTx) if validSignature(initialTx, tx) =>
          counterParty.foreach(acct => acct.unsignedTxs.remove(initialTx.hash))
          chainCache.get(tx.senderPubKey).foreach(acct => acct.balance -= tx.amount)
          chainCache.get(tx.counterPartyPubKey).foreach(acct => acct.balance += tx.amount)
        case None =>
          counterParty.foreach(acct => acct.unsignedTxs.update(tx.hash, tx))
      }
    }
  }
}


