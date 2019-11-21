package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect._
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.domain.consensus.ConsensusService
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Edge, Schema, Transaction, TransactionCacheData}

class TransactionService[F[_]: Concurrent](val transactionChainService: TransactionChainService[F], dao: DAO)
    extends ConsensusService[F, TransactionCacheData] {

  private val logger = Slf4jLogger.getLogger[F]

  protected[domain] val pending = PendingTransactionsMemPool[F](transactionChainService)

  override def metricRecordPrefix: Option[String] = "Transaction".some

  override def accept(tx: TransactionCacheData, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(tx, cpc)
      .flatMap(_ => transactionChainService.acceptTransaction(tx.transaction))
      .void
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("transactionAccepted"))
      .flatTap(_ => logger.debug(s"Accepting transaction=${tx.hash}"))

  override def pullForConsensus(maxCount: Int): F[List[TransactionCacheData]] =
    super.pullForConsensus(maxCount).flatMap { txs =>
      if (txs.isEmpty) createDummyTransactions(1) else txs.pure[F]
    }

  def createDummyTransactions(count: Int): F[List[TransactionCacheData]] =
    List
      .fill(count) {
        val keyPair = KeyUtils.makeKeyPair()
        TransactionService
          .createDummyTransaction(
            keyPair.getPublic.toId.address,
            KeyUtils.makeKeyPair().getPublic.toId.address,
            keyPair
          )(
            transactionChainService
          )
          .map(TransactionCacheData(_))
      }
      .sequence
      .flatMap(_.traverse(tx => inConsensus.put(tx.hash, tx)))

  def removeConflicting(txs: List[String]): F[Unit] =
    pending.remove(txs.toSet) >> unknown.remove(txs.toSet)

  def createTransaction(
    src: String,
    dst: String,
    lastTxRef: LastTransactionRef,
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  ): F[Transaction] =
    TransactionService.createAndSetTransaction(src, dst, lastTxRef, amount, keyPair, normalized, dummy)(transactionChainService)

  def createDummyTransaction(src: String, dst: String, keyPair: KeyPair): F[Transaction] =
    TransactionService.createDummyTransaction(src, dst, keyPair)(transactionChainService)

  def receiveTransaction(tx: Transaction): F[TransactionCacheData] =
    transactionChainService
      .setLastTransaction(tx.edge, false)
      .flatMap(tx => put(TransactionCacheData(tx)))
}

object TransactionService {

  def apply[F[_]: Concurrent](transactionChainService: TransactionChainService[F], dao: DAO) =
    new TransactionService[F](transactionChainService, dao)

  def createTransactionEdge(
    src: String,
    dst: String,
    lastTxRef: LastTransactionRef,
    amount: Long,
    keyPair: KeyPair,
    fee: Option[Long] = None,
    normalized: Boolean = true
  ): Edge[TransactionEdgeData] = {
    val amountToUse = if (normalized) amount * Schema.NormalizationFactor else amount

    val txData = TransactionEdgeData(amountToUse, lastTxRef, fee)

    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(keyPair)

    Edge(oe, soe, txData)
  }

  def createAndSetTransaction[F[_]: Concurrent](
    src: String,
    dst: String,
    lastTxRef: LastTransactionRef,
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  )(transactionChainService: TransactionChainService[F]): F[Transaction] = {
    val soe = createTransactionEdge(src, dst, lastTxRef, amount, keyPair)
    transactionChainService.setLastTransaction(soe, dummy)
  }


  def createDummyTransaction[F[_]: Concurrent](src: String, dst: String, keyPair: KeyPair, prevTxRef: String = "dummy", ordinal: Long = 0L)(
    transactionChainService: TransactionChainService[F]
  ): F[Transaction] =
    createAndSetTransaction[F](src, dst, LastTransactionRef.empty,0L, keyPair, true)(transactionChainService)

  def receiveTransaction[F[_]: Concurrent](tx: Transaction)(transactionChainService: TransactionChainService[F]): F[Transaction] =
    transactionChainService.setLastTransaction(tx.edge, false)
}
