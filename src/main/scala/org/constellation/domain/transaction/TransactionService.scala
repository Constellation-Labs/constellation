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
import org.constellation.storage.RateLimiting

class TransactionService[F[_]: Concurrent](
  val transactionChainService: TransactionChainService[F],
  rateLimiting: RateLimiting[F],
  dao: DAO
) extends ConsensusService[F, TransactionCacheData] {

  private val logger = Slf4jLogger.getLogger[F]

  protected[domain] val pending = PendingTransactionsMemPool[F](transactionChainService, rateLimiting)

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

  def applyAfterRedownload(tx: TransactionCacheData, cpc: Option[CheckpointCache]): F[Unit] =
    super
      .accept(tx, cpc)
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("transactionAccepted"))
      .flatTap(_ => dao.metrics.incrementMetricAsync[F]("transactionAcceptedFromRedownload"))
      .flatTap(_ => logger.debug(s"Accepting transaction after redownload with hash=${tx.hash}"))

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
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  ): F[Transaction] =
    TransactionService.createTransaction(src, dst, amount, keyPair, normalized, dummy)(
      transactionChainService
    )

  def createDummyTransaction(src: String, dst: String, keyPair: KeyPair): F[Transaction] =
    TransactionService.createDummyTransaction(src, dst, keyPair)(transactionChainService)

  override def returnToPending(as: Seq[String]): F[List[TransactionCacheData]] =
    as.toList
      .traverse(inConsensus.lookup)
      .map(_.flatten)
      .flatMap { txs =>
        txs.traverse(tx => withLock("inConsensusUpdate", inConsensus.remove(tx.hash))) >>
          txs.filterNot(_.transaction.isDummy).traverse(put)
      }
      .flatTap(
        txs =>
          if (txs.nonEmpty) logger.info(s"TransactionService returningToPending with hashes=${txs.map(_.hash)}")
          else Sync[F].unit
      )

}

object TransactionService {

  def apply[F[_]: Concurrent](
    transactionChainService: TransactionChainService[F],
    rateLimiting: RateLimiting[F],
    dao: DAO
  ) =
    new TransactionService[F](transactionChainService, rateLimiting, dao)

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

  def createTransaction[F[_]: Concurrent](
    src: String,
    dst: String,
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  )(transactionChainService: TransactionChainService[F]): F[Transaction] =
    if (dummy) transactionChainService.checkDummyTransaction(src, dst, amount, keyPair, dummy, normalized = normalized)
    else transactionChainService.createAndSetLastTransaction(src, dst, amount, keyPair, dummy, normalized = normalized)

  def createDummyTransaction[F[_]: Concurrent](src: String, dst: String, keyPair: KeyPair)(
    transactionChainService: TransactionChainService[F]
  ): F[Transaction] =
    createTransaction[F](src, dst, 0L, keyPair, normalized = false, dummy = true)(
      transactionChainService
    )
}
