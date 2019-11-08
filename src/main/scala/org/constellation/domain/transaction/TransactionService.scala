package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.domain.consensus.{ConsensusService, ConsensusStatus}
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Edge, Schema, Transaction, TransactionCacheData}

class TransactionService[F[_]: Concurrent](val transactionChainService: TransactionChainService[F], dao: DAO)
    extends ConsensusService[F, TransactionCacheData] {

  private val logger = Slf4jLogger.getLogger[F]

  private val pendingSemaphore = ConstellationExecutionContext.createSemaphore()
  protected[domain] val pending = PendingTransactionsMemPool[F](transactionChainService, pendingSemaphore)

  override def metricRecordPrefix: Option[String] = "Transaction".some

  override def accept(tx: TransactionCacheData, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(tx, cpc)
      .flatMap(_ => transactionChainService.acceptTransaction(tx.transaction))
      .void
      .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("transactionAccepted")))
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
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  ): F[Transaction] =
    TransactionService.createTransaction(src, dst, amount, keyPair, normalized, dummy)(transactionChainService)

}

object TransactionService {

  def apply[F[_]: Concurrent](transactionChainService: TransactionChainService[F], dao: DAO) =
    new TransactionService[F](transactionChainService, dao)

  def createTransaction[F[_]: Concurrent](
    src: String,
    dst: String,
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  )(transactionChainService: TransactionChainService[F]): F[Transaction] = {
    val amountToUse = if (normalized) amount * Schema.NormalizationFactor else amount

    val txData = TransactionEdgeData(amount = amountToUse)

    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(keyPair)

    transactionChainService.setLastTransaction(Edge(oe, soe, txData), dummy)
  }

  def createDummyTransaction[F[_]: Concurrent](src: String, dst: String, keyPair: KeyPair)(
    transactionChainService: TransactionChainService[F]
  ): F[Transaction] =
    createTransaction[F](src, dst, 0L, keyPair, normalized = false, dummy = true)(transactionChainService)
}
