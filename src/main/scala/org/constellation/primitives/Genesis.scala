package org.constellation.primitives

import java.security.KeyPair

import better.files.File
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.{Logger, SelfAwareStructuredLogger}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.ConsensusStatus
import org.constellation.util.AccountBalance
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.util.{Failure, Success, Try}

object Genesis extends StrictLogging {

  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  final val CoinBaseHash = "coinbase"
  final val DebtKey = KeyUtils.makeKeyPair()

  private final val GenesisTips = Seq(
    TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
    TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash)
  )

  def createGenesisTransaction(keyPair: KeyPair, allocAccountBalances: Seq[AccountBalance]): Seq[Transaction] =
    allocAccountBalances.map(t => createTransaction(DebtKey.address, t.accountHash, t.balance, DebtKey)) :+
      createTransaction(DebtKey.address, keyPair.getPublic.toId.address, 4e9.toLong, keyPair)

  def createGenesisBlock(keyPair: KeyPair, allocAccountBalances: Seq[AccountBalance]): CheckpointBlock =
    CheckpointBlock.createCheckpointBlock(createGenesisTransaction(keyPair, allocAccountBalances), GenesisTips)(keyPair)

  def start()(implicit dao: DAO): Unit = {
    // TODO: Remove initial distribution
    val fakeIdToGenerateTips = KeyUtils.makeKeyPair().getPublic.toId
    val go = createGenesisAndInitialDistributionDirect(
      dao.selfAddressStr,
      Set(fakeIdToGenerateTips),
      dao.keyPair,
      dao.nodeConfig.allocAccountBalances
    )
    acceptGenesis(go, setAsTips = true)
  }

  // TODO: Get rid of this, need to add edge case for handling tips at beginning to avoid this
  def createDistribution(
    selfAddressStr: String,
    ids: Seq[Id],
    genesisSOE: SignedObservationEdge,
    keyPair: KeyPair
  ): CheckpointBlock = {

    val distr = ids.map { id =>
      createTransaction(selfAddressStr, id.address, 1e6.toLong, keyPair)
    }

    CheckpointBlock.createCheckpointBlock(
      distr,
      Seq(
        TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash)),
        TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash))
      )
    )(keyPair)

  }

  /**
    * Build genesis tips and example distribution among initial nodes
    *
    * @param ids: Initial node public keys
    * @return : Resolved edges for state update
    */
  def createGenesisAndInitialDistributionDirect(
    selfAddressStr: String,
    ids: Set[Id],
    keyPair: KeyPair,
    allocAccountBalances: Seq[AccountBalance] = Seq.empty
  ): GenesisObservation = {

    val genesisCBO = createGenesisBlock(keyPair, allocAccountBalances)
    val soe = genesisCBO.soe

    val distr1CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)
    val distr2CBO = createDistribution(selfAddressStr, ids.toSeq, soe, keyPair)

    GenesisObservation(genesisCBO, distr1CBO, distr2CBO)
  }

  def createGenesisAndInitialDistribution(selfAddressStr: String, ids: Set[Id], keyPair: KeyPair): GenesisObservation =
    createGenesisAndInitialDistributionDirect(selfAddressStr, ids, keyPair)

  def acceptGenesis(go: GenesisObservation, setAsTips: Boolean = false)(implicit dao: DAO): Unit = {
    // Store hashes for the edges

    (go.genesis.storeSOE() *>
      go.initialDistribution.storeSOE() *>
      go.initialDistribution2.storeSOE() *>
      IO(go.genesis.store(CheckpointCache(Some(go.genesis), height = Some(Height(0, 0))))) *>
      IO(go.initialDistribution.store(CheckpointCache(Some(go.initialDistribution), height = Some(Height(1, 1))))) *>
      IO(go.initialDistribution2.store(CheckpointCache(Some(go.initialDistribution2), height = Some(Height(1, 1))))))
      .unsafeRunSync()

    // Store the balance for the genesis TX minus the distribution along with starting rep score.
    go.genesis.transactions.find(_.dst.address == dao.selfAddressStr).foreach { rtx =>
      val bal = rtx.amount - (go.initialDistribution.transactions.map { _.amount }.sum * 2)
      dao.addressService
        .put(rtx.dst.hash, AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal))
        .unsafeRunSync()
    }

    // Store the balance for the initial distribution addresses along with starting rep score.
    go.initialDistribution.transactions.foreach { t =>
      val bal = t.amount * 2
      dao.addressService
        .put(t.dst.hash, AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal))
        .unsafeRunSync()
    }

    // Store the balance for the initial transaction from alloc file.
    go.genesis.transactions.filter(_.dst.address != dao.selfAddressStr).foreach { rtx =>
      val bal = rtx.amount
      dao.addressService
        .put(rtx.dst.hash, AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal))
        .unsafeRunSync()
    }

    dao.genesisObservation = Some(go)
    dao.genesisBlock = Some(go.genesis)

    /*
    // Dumb way to set these as active tips, won't pass a double validation but no big deal.
    checkpointMemPool(go.initialDistribution.baseHash) = go.initialDistribution
    checkpointMemPool(go.initialDistribution2.baseHash) = go.initialDistribution2
    checkpointMemPoolThresholdMet(go.initialDistribution.baseHash) = go.initialDistribution -> 0
    checkpointMemPoolThresholdMet(go.initialDistribution2.baseHash) = go.initialDistribution2 -> 0
     */

    dao.metrics.updateMetric("genesisAccepted", "true")

    if (setAsTips) {
      List(go.initialDistribution, go.initialDistribution2)
        .map(dao.concurrentTipService.update(_, Height(1, 1), isGenesis = true))
        .sequence
        .unsafeRunSync()
    }
    storeTransactions(go)

    dao.metrics.updateMetric("genesisHash", go.genesis.soeHash)

    GenesisObservationWriter
      .writeToDisk(go, dao.genesisObservationPath)(IO.contextShift(ConstellationExecutionContext.bounded))
      .flatTap {
        case Success(value)     => Logger[IO].info(s"Genesis observation saved successfully ${value.pathAsString}")
        case Failure(exception) => Logger[IO].error(s"Cannot save genesis observation to disk ${exception.getMessage}")
      }
      .unsafeRunSync()
  }

  private def storeTransactions(genesisObservation: GenesisObservation)(implicit dao: DAO): Unit =
    Seq(genesisObservation.genesis, genesisObservation.initialDistribution, genesisObservation.initialDistribution2).flatMap {
      cb =>
        cb.transactions
          .map(tx => TransactionCacheData(transaction = tx, cbBaseHash = Some(cb.baseHash)))
          .map(tcd => dao.transactionService.put(tcd, ConsensusStatus.Accepted))
    }.toList.sequence.void.unsafeRunSync()
}

object GenesisObservationWriter {

  final val FILE_NAME: String = "genesisObservation"

  def writeToDisk(genesisObservation: GenesisObservation, path: File)(implicit cs: ContextShift[IO]): IO[Try[File]] =
    cs.evalOn(ConstellationExecutionContext.unbounded) {
      IO(Try(File(path, FILE_NAME).writeByteArray(KryoSerializer.serializeAnyRef(genesisObservation))))
    }
}
