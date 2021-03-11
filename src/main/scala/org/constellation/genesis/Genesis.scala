package org.constellation.genesis

import cats.syntax.all._
import cats.effect.{IO, LiftIO, Sync}
import com.typesafe.scalalogging.StrictLogging
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.keytool.KeyUtils
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.edge.{EdgeHashType, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionCacheData}
import org.constellation.schema.{GenesisObservation, Height, PublicKeyExt}
import org.constellation.util.AccountBalance
import org.constellation.DAO

import java.security.KeyPair

object Genesis extends StrictLogging {

  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  final val Coinbase = "Nature loves courage. You make the commitment and nature will respond to that commitment " +
    "by removing impossible obstacles. Dream the impossible dream and the world will not grind you under, it will " +
    "lift you up. This is the trick. This is what all these teachers and philosophers who really counted, who really " +
    "touched the alchemical gold, this is what they understood. This is the shamanic dance in the waterfall. This is " +
    "how magick is done. By hurling yourself into the abyss and discovering it's a feather bed."

  private final val GenesisTips = Seq(
    TypedEdgeHash(Coinbase, EdgeHashType.CheckpointHash),
    TypedEdgeHash(Coinbase, EdgeHashType.CheckpointHash)
  )

  private def createDistributionTransactions[F[_]: Sync: LiftIO](
    allocAccountBalances: Seq[AccountBalance],
    coinbaseKey: KeyPair
  )(dao: DAO): F[List[Transaction]] =
    allocAccountBalances.toList
      .traverse(
        ab =>
          Sync[F].delay(logger.info(s"Account Balance set : $ab")) >>
            LiftIO[F].liftIO(
              dao.transactionService
                .createTransaction(
                  LastTransactionRef.empty.prevHash,
                  ab.accountHash,
                  ab.balance,
                  coinbaseKey,
                  normalized = false
                )
            )
      )

  private def createGenesisBlock(transactions: Seq[Transaction], coinbaseKey: KeyPair): CheckpointBlock =
    CheckpointBlock.createCheckpointBlock(transactions, GenesisTips)(coinbaseKey)

  private def createEmptyBlockFromGenesis[F[_]: Sync: LiftIO](
    genesisSOE: SignedObservationEdge,
    coinbaseKey: KeyPair
  )(dao: DAO): F[CheckpointBlock] =
    for {
      dummyTransactionSrc <- Sync[F].delay(KeyUtils.makeKeyPair().getPublic.toId.address)
      dummyTransactionDst <- Sync[F].delay(KeyUtils.makeKeyPair().getPublic.toId.address)
      dummyTx <- LiftIO[F].liftIO {
        dao.transactionService.createDummyTransaction(dummyTransactionSrc, dummyTransactionDst, coinbaseKey)
      }
      cb <- Sync[F].delay {
        CheckpointBlock.createCheckpointBlock(
          Seq(dummyTx),
          Seq(
            TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash)),
            TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash))
          )
        )(coinbaseKey)
      }
    } yield cb

  // TODO: Make F[Unit]
  def createGenesisObservation[F[_]: Sync: LiftIO](
    allocAccountBalances: Seq[AccountBalance] = Seq.empty
  )(dao: DAO): F[GenesisObservation] =
    for {
      // TODO: That key should be provided externally by the genesis creator.
      // We can use Node's KeyPair as well.
      coinbaseKey <- Sync[F].delay(KeyUtils.makeKeyPair())
      genesis <- createDistributionTransactions[F](allocAccountBalances, coinbaseKey)(dao)
        .map(createGenesisBlock(_, coinbaseKey))
      emptyA <- createEmptyBlockFromGenesis[F](genesis.soe, coinbaseKey)(dao)
      emptyB <- createEmptyBlockFromGenesis[F](genesis.soe, coinbaseKey)(dao)
    } yield GenesisObservation(genesis, emptyA, emptyB)

  def start[F[_]: Sync: LiftIO](dao: DAO): F[Unit] =
    for {
      _ <- LiftIO[F].liftIO(dao.cluster.setParticipatedInGenesisFlow(true))
      _ <- LiftIO[F].liftIO(dao.cluster.setParticipatedInRollbackFlow(false))
      _ <- LiftIO[F].liftIO(dao.cluster.setJoinedAsInitialFacilitator(true))
      _ <- LiftIO[F].liftIO(dao.cluster.setOwnJoinedHeight(0L))
      genesisObservation <- createGenesisObservation(dao.nodeConfig.allocAccountBalances)(dao)
      _ <- acceptGenesis(genesisObservation)(dao)
    } yield ()

  // TODO: Make F[Unit]
  def acceptGenesis[F[_]: Sync: LiftIO](go: GenesisObservation, setAsTips: Boolean = true)(
    dao: DAO
  ): F[Unit] =
    // Store hashes for the edges
    for {
      _ <- Sync[F].delay(logger.debug("Started genesis acceptance!"))
      genesisBlock = CheckpointCache(go.genesis, height = Some(Height(0, 0)))
      initialBlock1 = CheckpointCache(go.initialDistribution, height = Some(Height(1, 1)))
      initialBlock2 = CheckpointCache(go.initialDistribution2, height = Some(Height(1, 1)))
      _ <- LiftIO[F].liftIO(dao.soeService.put(go.genesis.soeHash, go.genesis.soe))
      _ <- LiftIO[F].liftIO(dao.soeService.put(go.initialDistribution.soeHash, go.initialDistribution.soe))
      _ <- LiftIO[F].liftIO(dao.soeService.put(go.initialDistribution2.soeHash, go.initialDistribution2.soe))
      _ <- LiftIO[F].liftIO(dao.checkpointService.put(genesisBlock))
      _ <- LiftIO[F].liftIO(dao.checkpointService.put(initialBlock1))
      _ <- LiftIO[F].liftIO(dao.checkpointService.put(initialBlock2))
      _ <- Sync[F].delay(dao.recentBlockTracker.put(genesisBlock))
      _ <- Sync[F].delay(dao.recentBlockTracker.put(initialBlock1))
      _ <- Sync[F].delay(dao.recentBlockTracker.put(initialBlock2))
      // Changed it from setting balance one by one for every tx to setting them all at once with setAll
      genesisBalances = go.genesis.transactions.map { rtx =>
        val bal = rtx.amount
        rtx.dst.hash -> AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal)
      }.toMap
      _ <- LiftIO[F].liftIO(dao.addressService.setAll(genesisBalances))
      _ <- Sync[F].delay {
        dao.genesisObservation = Some(go)
        dao.genesisBlock = Some(go.genesis)
      }
      _ <- dao.metrics.updateMetricAsync("genesisAccepted", "true")
      _ <- if (setAsTips)
        List(go.initialDistribution, go.initialDistribution2)
          .traverse(cb => LiftIO[F].liftIO(dao.concurrentTipService.update(cb, Height(1, 1), isGenesis = true)))
          .void
      else
        Sync[F].unit
      _ <- storeTransactions(go)(dao)
      _ <- dao.metrics.updateMetricAsync("genesisHash", go.genesis.soeHash)
      _ <- LiftIO[F].liftIO { dao.genesisObservationStorage.write(go).value }.map {
        case Left(err) =>
          Sync[F].delay(logger.error(s"Cannot write genesis observation ${err.getMessage}")) >>
            Sync[F].raiseError(err)
        case Right(_) =>
          Sync[F].delay(logger.debug("Genesis observation saved successfully"))
      }
      _ <- LiftIO[F].liftIO(dao.cloudService.enqueueGenesis(go))
      /*
      // Dumb way to set these as active tips, won't pass a double validation but no big deal.
      checkpointMemPool(go.initialDistribution.baseHash) = go.initialDistribution
      checkpointMemPool(go.initialDistribution2.baseHash) = go.initialDistribution2
      checkpointMemPoolThresholdMet(go.initialDistribution.baseHash) = go.initialDistribution -> 0
      checkpointMemPoolThresholdMet(go.initialDistribution2.baseHash) = go.initialDistribution2 -> 0
     */
    } yield ()

  private def storeTransactions[F[_]: Sync: LiftIO](
    genesisObservation: GenesisObservation
  )(dao: DAO): F[Unit] =
    List(genesisObservation.genesis, genesisObservation.initialDistribution, genesisObservation.initialDistribution2)
      .flatMap(
        cb =>
          cb.transactions
            .map(tx => TransactionCacheData(transaction = tx, cbBaseHash = Some(cb.baseHash)))
      )
      .traverse(tcd => LiftIO[F].liftIO(dao.transactionService.accept(tcd)))
      .void
}
