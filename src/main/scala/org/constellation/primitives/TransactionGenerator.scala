package org.constellation.primitives

import java.security.KeyPair
import java.util.concurrent.atomic.AtomicInteger

import cats.data.EitherT
import cats.effect.{Async, Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.p2p.{Cluster, PeerData}
import org.constellation.primitives.Schema.NodeState.NodeState
import org.constellation.primitives.Schema.{AddressCacheData, Id, NodeState, NodeType}
import org.constellation.storage.ConsensusStatus.ConsensusStatus
import org.constellation.storage.transactions.TransactionStatus.TransactionStatus
import org.constellation.storage.transactions.{TransactionGossiping, TransactionStatus}
import org.constellation.storage.{AddressService, ConsensusStatus, TransactionService}
import org.constellation.util.Distance
import org.constellation.{ConstellationContextShift, ConstellationExecutionContext, DAO}

import scala.util.{Failure, Random, Success}

class TransactionGenerator[F[_]: Concurrent: Logger](
  addressService: AddressService[F],
  transactionGossiping: TransactionGossiping[F],
  transactionService: TransactionService[F],
  cluster: Cluster[F],
  dao: DAO
) {

  private final val roundCounter = new AtomicInteger(0)
  private final val emptyRounds = dao.processingConfig.emptyTransactionsRounds
  private final val transactionsRounds = dao.processingConfig.amountTransactionsRounds

  val multiAddressGenerationMode = false
  val requiredBalance = 10000000
  val rangeAmount = 1000

  def generate(): EitherT[F, TransactionGeneratorError, Unit] =
    for {
      _ <- validateNodeState(requiredState = NodeState.Ready)
      _ <- EitherT.fromEither[F](validateNodeIsPermitToGenerateRandomTransaction)

      addressData <- EitherT.liftF(getAddressData)
      _ <- EitherT.fromEither[F](validateNodeHasBalance(addressData))

      pendingTransactionCount <- EitherT.liftF(getCountTransactionsWithStatus(ConsensusStatus.Pending))
      _ <- EitherT.fromEither[F](validatePendingNumberOfTransactionIsLessThanMemPool(pendingTransactionCount))

      readyPeers <- EitherT.liftF(getReadyPeers)
      _ <- EitherT.fromEither[F](validateNodeHasPeersOrIsGenesisNode(readyPeers))

      numberOfTransaction <- EitherT.liftF(numberOfTransaction(readyPeers))
      _ <- EitherT.liftF(generateTransactions(readyPeers, numberOfTransaction))
    } yield ()

  private def generateTransactions(peers: Seq[(Id, PeerData)], numberOfTransaction: Int) = {
    val transaction = for {
      transaction <- generateTransaction(peers)
      _ <- dao.metrics.incrementMetricAsync("signaturesPerformed")
      _ <- dao.metrics.incrementMetricAsync("randomTransactionsGenerated")
      _ <- dao.metrics.incrementMetricAsync("sentTransactions")
      _ <- putTransaction(transaction)

      transactionCacheData <- observeTransaction(transaction)
      _ <- Logger[F].debug(
        s"Rebroadcast transaction=${transactionCacheData.transaction.hash}, initial path=${transactionCacheData.path}"
      )
      peers <- selectPeers(transactionCacheData)
      peerData <- peerData(peers)

      _ <- broadcastTransaction(transactionCacheData, peerData)
      _ <- dao.metrics.incrementMetricAsync("transactionGossipingSent")
      lightPeers <- peerDataNodeTypeLight()
      _ <- if (lightPeers.nonEmpty) broadcastLightNode(lightPeers, transaction) else Sync[F].unit
    } yield ()

    List.fill(numberOfTransaction)(transaction).sequence
  }

  private def broadcastLightNode(lightPeers: Map[Id, PeerData], tx: Transaction) = {
    val broadcastLightNode: F[Either[String, String]] = Async[F].async { cb =>
      lightPeers
        .minBy(p ⇒ Distance.calculate(p._1, dao.id))
        ._2
        .client
        .put("transaction", TransactionGossip(tx))
        .onComplete {
          case Success(value) => cb(Right(value.body))
          case Failure(error) => cb(Left(error))
        }(ConstellationExecutionContext.edge)
    }
    for {
      _ <- broadcastLightNode
      _ <- dao.metrics.incrementMetricAsync("transactionPut")
      _ <- dao.metrics.incrementMetricAsync("transactionPutToLightNode")
    } yield ()
  }

  private def generateTransaction(peers: Seq[(Id, PeerData)]): F[Transaction] =
    if (multiAddressGenerationMode) generateMultipleAddressTransaction(peers)
    else generateSingleAddressTransaction(peers)

  private def generateSingleAddressTransaction(peers: Seq[(Id, PeerData)]): F[Transaction] =
    Sync[F].delay {
      createTransaction(
        dao.selfAddressStr,
        randomAddressFromPeers(peers),
        randomAmount(rangeAmount),
        dao.keyPair,
        normalized = false
      )
    }

  private def generateMultipleAddressTransaction(peers: Seq[(Id, PeerData)]): F[Transaction] =
    balancesForAddresses.map(
      addresses =>
        if (checkAddressesHaveSufficient(addresses)) generateMultipleAddressTransactionWithSufficent(addresses, peers)
        else generateMultipleAddressTransactionWithoutSufficent(addresses, peers)
    )

  private def generateMultipleAddressTransactionWithSufficent(
    addresses: Seq[(String, Option[AddressCacheData])],
    peers: Seq[(Id, PeerData)]
  ): Transaction =
    createTransaction(dao.selfAddressStr, randomAddressFrom(addresses), randomAmount(rangeAmount), dao.keyPair)

  private def generateMultipleAddressTransactionWithoutSufficent(
    addresses: Seq[(String, Option[AddressCacheData])],
    peers: Seq[(Id, PeerData)]
  ): Transaction = {
    val source = getSourceAddressForTxWithoutSufficient(addresses)
    createTransaction(
      source,
      randomAddressFromPeers(peers),
      randomAmount(rangeAmount),
      keyPairForSource(source),
      normalized = false
    )
  }

  private def peerDataNodeTypeLight(): F[Map[Id, PeerData]] =
    LiftIO[F].liftIO(dao.peerInfo(NodeType.Light))

  private def broadcastTransaction(tcd: TransactionCacheData, peerData: List[PeerData]) = {
    val contextShift: ContextShift[IO] = ConstellationContextShift.edge
    LiftIO[F].liftIO(contextShift.shift *> peerData.traverse(_.client.putAsync("transaction", TransactionGossip(tcd))))
  }

  private def peerData(peers: Set[Schema.Id]): F[List[PeerData]] =
    LiftIO[F].liftIO(dao.peerInfo(NodeType.Full).map(_.filterKeys(peers.contains).values.toList))

  private def observeTransaction(transaction: Transaction): F[TransactionCacheData] =
    transactionGossiping.observe(TransactionCacheData(transaction))

  private def selectPeers(transactionCacheData: TransactionCacheData): F[Set[Schema.Id]] =
    transactionGossiping.selectPeers(transactionCacheData)(scala.util.Random)

  private def putTransaction(tx: Transaction): F[TransactionCacheData] =
    transactionService.put(TransactionCacheData(tx, path = Set(dao.id)))

  private def numberOfTransaction(peers: Seq[(Id, PeerData)]): F[Int] =
    roundCounter.getAndIncrement() match {
      case x if x < transactionsRounds                 => dao.processingConfig.maxTransactionsPerRound.pure[F]
      case y if y < (transactionsRounds + emptyRounds) => 0.pure[F]
      case _ =>
        roundCounter.set(0)
        0.pure[F]
    }

  private def keyPairForSource(sourceAddress: String): KeyPair =
    (dao.addressToKeyPair + (dao.selfAddressStr -> dao.keyPair))(sourceAddress)

  private def checkAddressesHaveSufficient(balancesAddresses: Seq[(String, Option[AddressCacheData])]): Boolean =
    balancesAddresses.forall { _._2.exists(_.balance > requiredBalance) }

  private def balancesForAddresses: F[List[(String, Option[AddressCacheData])]] =
    dao.addresses.toList.traverse(address => addressService.lookup(address).map(address -> _))

  private def getSourceAddressForTxWithoutSufficient(address: Seq[(String, Option[AddressCacheData])]) = {
    val addresses = dao.addresses
    val historyCheckPassable = address.forall(_._2.exists(_.balanceByLatestSnapshot > requiredBalance))
    if (historyCheckPassable && addresses.nonEmpty) Random.shuffle(addresses :+ dao.selfAddressStr).head
    else dao.selfAddressStr
  }

  private def randomAmount(n: Int): Long = Random.nextInt(n).toLong + 1L

  private def randomAddressFromPeers(peers: Seq[(Id, PeerData)]): String =
    if (peers.isEmpty && dao.nodeConfig.isGenesisNode) dao.dummyAddress
    else peers(Random.nextInt(peers.size))._1.address

  private def randomAddressFrom(addresses: Seq[(String, Option[AddressCacheData])]): String =
    if (addresses.isEmpty) dao.dummyAddress
    else Random.shuffle(addresses.filterNot(_._2.exists(_.balance > requiredBalance))).head._1

  private def getReadyPeers: F[Seq[(Id, PeerData)]] =
    for {
      readyPeers <- LiftIO[F].liftIO(
        dao.readyPeers.map(
          _.toSeq.filter(
            peersNotOlderThan(System.currentTimeMillis() - (dao.processingConfig.minPeerTimeAddedSeconds * 1000))
          )
        )
      )
      _ <- dao.metrics.updateMetricAsync("numPeersOnDAOThatAreReady", readyPeers.size.toString)

      peerInfo <- LiftIO[F].liftIO(dao.peerInfo)
      _ <- dao.metrics.updateMetricAsync("numPeersOnDAO", peerInfo.size.toString)
    } yield readyPeers

  private def peersNotOlderThan(timeInMillis: Long)(m: (Id, PeerData)): Boolean =
    m._2.peerMetadata.timeAdded < timeInMillis

  private def getCountTransactionsWithStatus(transactionStatus: ConsensusStatus): F[Long] =
    transactionService
      .count(transactionStatus)
      .flatTap(c => dao.metrics.updateMetricAsync("transactionPendingSize", c.toString))

  private def getAddressData: F[Option[AddressCacheData]] =
    addressService.lookup(dao.selfAddressStr)

  private def validateNodeHasPeersOrIsGenesisNode(peers: Seq[(Id, PeerData)]): Either[TransactionGeneratorError, Unit] =
    if (dao.nodeConfig.isGenesisNode || peers.nonEmpty) Right(()) else Left(NodeIsNotGenesisNodeAndPeersAreEmpty)

  private def validateNodeHasBalance(address: Option[AddressCacheData]): Either[TransactionGeneratorError, Unit] =
    address match {
      case Some(a: AddressCacheData) =>
        if (a.balanceByLatestSnapshot > requiredBalance) Right(()) else Left(NodeHasNotRequiredBalance)
      case None => Left(NodeHasNotRequiredBalance)
    }

  private def validatePendingNumberOfTransactionIsLessThanMemPool(
    pendingCount: Long
  ): Either[TransactionGeneratorError, Unit] =
    if (pendingCount < dao.processingConfig.maxMemPoolSize) Right(()) else Left(NodeHasToManyPendingTransactions)

  private def validateNodeState(requiredState: NodeState): EitherT[F, TransactionGeneratorError, Unit] =
    EitherT {
      cluster.getNodeState.map { nodeState =>
        if (nodeState == requiredState) {
          ().asRight[TransactionGeneratorError]
        } else {
          NodeIsNotInRequiredState.asLeft[Unit]
        }
      }
    }

  private def validateNodeIsPermitToGenerateRandomTransaction: Either[TransactionGeneratorError, Unit] =
    if (dao.generateRandomTX) Right(()) else Left(NodeIsNotPermitToGenerateRandomTransaction)

}

object TransactionGenerator {

  def apply[F[_]: Concurrent: Logger](
    addressService: AddressService[F],
    transactionGossiping: TransactionGossiping[F],
    transactionService: TransactionService[F],
    cluster: Cluster[F],
    dao: DAO
  ) = new TransactionGenerator[F](addressService, transactionGossiping, transactionService, cluster, dao)
}

sealed trait TransactionGeneratorError

object NodeIsNotInRequiredState extends TransactionGeneratorError
object NodeIsNotPermitToGenerateRandomTransaction extends TransactionGeneratorError
object NodeHasNotRequiredBalance extends TransactionGeneratorError
object NodeHasToManyPendingTransactions extends TransactionGeneratorError
object NodeIsNotGenesisNodeAndPeersAreEmpty extends TransactionGeneratorError