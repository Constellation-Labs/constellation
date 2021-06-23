package org.constellation.genesis

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.domain.checkpointBlock.CheckpointStorageAlgebra
import org.constellation.domain.cloud.CloudService.CloudServiceEnqueue
import org.constellation.domain.cluster.NodeStorageAlgebra
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.domain.genesis.GenesisStorageAlgebra
import org.constellation.domain.transaction.TransactionService
import org.constellation.genesis.Genesis.{CoinbaseKey, GenesisTips}
import org.constellation.keytool.KeyUtils
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.edge.{EdgeHashType, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionCacheData}
import org.constellation.schema.{GenesisObservation, Height, PublicKeyExt}
import org.constellation.storage.AddressService
import org.constellation.util.{AccountBalance, Metrics}

class Genesis[F[_]: Concurrent](
  genesisStorage: GenesisStorageAlgebra[F],
  transactionService: TransactionService[F],
  nodeStorage: NodeStorageAlgebra[F],
  nodeConfig: NodeConfig,
  checkpointStorage: CheckpointStorageAlgebra[F],
  addressService: AddressService[F],
  genesisObservationStorage: GenesisObservationLocalStorage[F],
  cloudService: CloudServiceEnqueue[F],
  metrics: Metrics
) {
  implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  def acceptGenesis(go: GenesisObservation, setAsTips: Boolean = true): F[Unit] = {
    val genesisBlock = CheckpointCache(go.genesis, height = Height(0, 0))
    val initialBlock1 = CheckpointCache(go.initialDistribution, height = Height(1, 1))
    val initialBlock2 = CheckpointCache(go.initialDistribution2, height = Height(1, 1))

    val putBlocks = checkpointStorage.persistCheckpoint(genesisBlock) >>
      checkpointStorage.persistCheckpoint(initialBlock1) >>
      checkpointStorage.persistCheckpoint(initialBlock2)

    val acceptBlocks = checkpointStorage.acceptCheckpoint(genesisBlock.checkpointBlock.soeHash) >>
      checkpointStorage.acceptCheckpoint(initialBlock1.checkpointBlock.soeHash) >>
      checkpointStorage.acceptCheckpoint(initialBlock2.checkpointBlock.soeHash)

    val setBalances = go.genesis.transactions.toList.traverse { rtx =>
      val bal = rtx.amount
      addressService
        .set(rtx.dst.hash, AddressCacheData(bal, bal, Some(1000d), balanceByLatestSnapshot = bal))
    }

    for {
      _ <- storeTransactions(go)
      _ <- putBlocks
      _ <- acceptBlocks
      _ <- setBalances
      _ <- metrics.updateMetricAsync[F]("genesisAccepted", "true")
      _ <- if (setAsTips) {
        List(go.initialDistribution, go.initialDistribution2)
          .map(_.soeHash)
          .traverse(checkpointStorage.addTip)
      } else Sync[F].unit
      _ <- metrics.updateMetricAsync[F]("genesisHash", go.genesis.soeHash)
      _ <- genesisStorage.setGenesisObservation(go)
      _ <- genesisObservationStorage
        .write(go)
        .fold(
          err => logger.error(s"Cannot write genesis observation ${err.getMessage}"),
          _ => logger.debug("Genesis observation saved successfully")
        )
      _ <- cloudService.enqueueGenesis(go)
    } yield ()
  }

  def start(): F[Unit] =
    for {
      _ <- nodeStorage.setParticipatedInGenesisFlow(true)
      _ <- nodeStorage.setParticipatedInRollbackFlow(false)
      _ <- nodeStorage.setJoinedAsInitialFacilitator(true)
      _ <- nodeStorage.setOwnJoinedHeight(0L)
      _ <- metrics.updateMetricAsync[F]("cluster_ownJoinedHeight", 0L)
      genesisObservation <- createGenesisObservation(nodeConfig.allocAccountBalances)
      _ <- acceptGenesis(genesisObservation)
    } yield ()

  private def createGenesisObservation(
    allocAccountBalances: Seq[AccountBalance] = Seq.empty
  ): F[GenesisObservation] =
    for {
      genesis <- createDistributionTransactions(allocAccountBalances)
        .map(createGenesisBlock)
      emptyA <- createEmptyBlockFromGenesis(genesis.soe)
      emptyB <- createEmptyBlockFromGenesis(genesis.soe)
    } yield GenesisObservation(genesis, emptyA, emptyB)

  private def createDistributionTransactions(allocAccountBalances: Seq[AccountBalance]): F[List[Transaction]] =
    allocAccountBalances.toList.traverse { ab =>
      logger.info(s"Account Balance set : $ab") >>
        transactionService.createTransaction(
          LastTransactionRef.empty.prevHash,
          ab.accountHash,
          ab.balance,
          CoinbaseKey,
          normalized = false
        )
    }

  private def createGenesisBlock(transactions: Seq[Transaction]): CheckpointBlock =
    CheckpointBlock.createCheckpointBlock(transactions, GenesisTips)(CoinbaseKey)

  private def createEmptyBlockFromGenesis(
    genesisSOE: SignedObservationEdge
  ): F[CheckpointBlock] = {
    val dummyTransactionSrc = KeyUtils.makeKeyPair.getPublic.toId.address
    val dummyTransactionDst = KeyUtils.makeKeyPair.getPublic.toId.address

    transactionService.createDummyTransaction(dummyTransactionSrc, dummyTransactionDst, CoinbaseKey).map { tx =>
      CheckpointBlock.createCheckpointBlock(
        Seq(tx),
        Seq(
          TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash)),
          TypedEdgeHash(genesisSOE.hash, EdgeHashType.CheckpointHash, Some(genesisSOE.baseHash))
        )
      )(CoinbaseKey)
    }
  }

  private def storeTransactions(genesisObservation: GenesisObservation): F[Unit] =
    List(genesisObservation.genesis, genesisObservation.initialDistribution, genesisObservation.initialDistribution2)
      .flatMap(
        cb =>
          cb.transactions
            .map(tx => TransactionCacheData(transaction = tx, cbBaseHash = Some(cb.soeHash)))
      )
      .traverse(transactionService.put(_, ConsensusStatus.Accepted))
      .void
}

object Genesis {
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
}
