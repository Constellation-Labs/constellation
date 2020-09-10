package org.constellation.primitives

import cats.effect.{Concurrent, ContextShift, IO, LiftIO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.util.AccountBalance
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

object Genesis extends StrictLogging {

  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  final val Coinbase = "Nature loves courage. You make the commitment and nature will respond to that commitment " +
    "by removing impossible obstacles. Dream the impossible dream and the world will not grind you under, it will " +
    "lift you up. This is the trick. This is what all these teachers and philosophers who really counted, who really " +
    "touched the alchemical gold, this is what they understood. This is the shamanic dance in the waterfall. This is " +
    "how magick is done. By hurling yourself into the abyss and discovering it's a feather bed."
  // TODO: That key should be provided externally by the genesis creator.
  // We can use Node's KeyPair as well.
  final val CoinbaseKey = KeyUtils.makeKeyPair()

  private final val GenesisTips = Seq(
    TypedEdgeHash(Coinbase, EdgeHashType.CheckpointHash),
    TypedEdgeHash(Coinbase, EdgeHashType.CheckpointHash)
  )

  private def createDistributionTransactions[F[_]: Concurrent](
    allocAccountBalances: Seq[AccountBalance]
  )(implicit dao: DAO): F[List[Transaction]] = LiftIO[F].liftIO {
    allocAccountBalances.toList
      .traverse(
        ab =>
          IO.delay(logger.info(s"Account Balance set : $ab")) >> dao.transactionService
            .createTransaction(
              LastTransactionRef.empty.prevHash,
              ab.accountHash,
              ab.balance,
              CoinbaseKey,
              normalized = false
            )
      )
  }

  private def createGenesisBlock(transactions: Seq[Transaction]): CheckpointBlock =
    CheckpointBlock.createCheckpointBlock(transactions, GenesisTips)(CoinbaseKey)

  private def createEmptyBlockFromGenesis[F[_]: Concurrent](
    genesisSOE: SignedObservationEdge
  )(implicit dao: DAO): F[CheckpointBlock] = LiftIO[F].liftIO {
    val dummyTransactionSrc = KeyUtils.makeKeyPair.getPublic.toId.address
    val dummyTransactionDst = KeyUtils.makeKeyPair.getPublic.toId.address

    dao.transactionService.createDummyTransaction(dummyTransactionSrc, dummyTransactionDst, CoinbaseKey).map { tx =>
      CheckpointBlock.createCheckpointBlock(
        Seq(tx),
        Seq(
          TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash)),
          TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash))
        )
      )(CoinbaseKey)
    }
  }

  // TODO: Make F[Unit]
  def createGenesisObservation(
    allocAccountBalances: Seq[AccountBalance] = Seq.empty
  )(implicit dao: DAO): GenesisObservation = {
    implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
    for {
      genesis <- createDistributionTransactions[IO](allocAccountBalances)
        .map(createGenesisBlock)
      emptyA <- createEmptyBlockFromGenesis[IO](genesis.soe)
      emptyB <- createEmptyBlockFromGenesis[IO](genesis.soe)
    } yield GenesisObservation(genesis, emptyA, emptyB)
  }.unsafeRunSync() // TODO: Get rid of unsafeRunSync and fix all the unit tests

  def start()(implicit dao: DAO): Unit = {
    val startIO = for {
      _ <- dao.cluster.setParticipatedInGenesisFlow(true)
      _ <- dao.cluster.setParticipatedInRollbackFlow(false)
      _ <- dao.cluster.setJoinedAsInitialFacilitator(true)
      _ <- dao.cluster.setOwnJoinedHeight(0L)
      genesisObservation = createGenesisObservation(dao.nodeConfig.allocAccountBalances)
      _ <- IO.delay {
        acceptGenesis(genesisObservation)
      }
    } yield genesisObservation

    startIO.unsafeRunSync()
  }

  // TODO: Make F[Unit]
  def acceptGenesis(go: GenesisObservation, setAsTips: Boolean = true)(implicit dao: DAO): Unit = {
    // Store hashes for the edges

    val genesisBlock = CheckpointCache(go.genesis, height = Some(Height(0, 0)))
    val initialBlock1 = CheckpointCache(go.initialDistribution, height = Some(Height(1, 1)))
    val initialBlock2 = CheckpointCache(go.initialDistribution2, height = Some(Height(1, 1)))

    (dao.soeService.put(go.genesis.soeHash, go.genesis.soe) >>
      dao.soeService
        .put(go.initialDistribution.soeHash, go.initialDistribution.soe) >>
      dao.soeService
        .put(go.initialDistribution2.soeHash, go.initialDistribution2.soe) >>
      dao.checkpointService.put(genesisBlock) >>
      dao.checkpointService.put(initialBlock1) >>
      dao.checkpointService.put(initialBlock2) >>
      IO(dao.recentBlockTracker.put(genesisBlock)) >>
      IO(dao.recentBlockTracker.put(initialBlock1)) >>
      IO(
        dao.recentBlockTracker
          .put(initialBlock2)
      ))
    // TODO: Get rid of unsafeRunSync
      .unsafeRunSync()

    go.genesis.transactions.foreach { rtx =>
      val bal = rtx.amount
      dao.addressService
        .set(rtx.dst.hash, AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal))
        // TODO: Get rid of unsafeRunSync
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
        .unsafeRunSync() // TODO: Get rid of unsafeRunSync
    }
    storeTransactions(go)

    dao.metrics.updateMetric("genesisHash", go.genesis.soeHash)

    // TODO: Get rid of unsafeRunSync

    dao.genesisObservationStorage
      .write(go)
      .fold(
        err => logger.error(s"Cannot write genesis observation ${err.getMessage}"),
        _ => logger.debug("Genesis observation saved successfully")
      )
      .unsafeRunSync()

    dao.cloudService.enqueueGenesis(go).unsafeRunSync()
  }

  private def storeTransactions(genesisObservation: GenesisObservation)(implicit dao: DAO): Unit =
    List(genesisObservation.genesis, genesisObservation.initialDistribution, genesisObservation.initialDistribution2)
      .flatMap(
        cb =>
          cb.transactions
            .map(tx => TransactionCacheData(transaction = tx, cbBaseHash = Some(cb.baseHash)))
      )
      .traverse(dao.transactionService.accept(_))
      .void
      .unsafeRunSync() // TODO: Get rid of unsafeRunSync
}
