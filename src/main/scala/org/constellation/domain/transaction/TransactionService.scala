package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect._
import cats.effect.concurrent.Semaphore
import cats.implicits._
import constellation._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.domain.consensus.{ConsensusService, ConsensusStatus}
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Edge, Schema, Transaction, TransactionCacheData}
import org.constellation.schema.HashGenerator

class TransactionService[F[_]: Concurrent: Logger](
  transactionChainService: TransactionChainService[F],
  implicit val hashGenerator: HashGenerator,
  dao: DAO
) extends ConsensusService[F, TransactionCacheData] {

  override def metricRecordPrefix: Option[String] = "Transaction".some

  protected[domain] val pending = new PendingTransactionsMemPool[F](Semaphore.in[IO, F](1).unsafeRunSync())

  override def accept(tx: TransactionCacheData, cpc: Option[CheckpointCache] = None): F[Unit] =
    super
      .accept(tx, cpc)
//      .flatMap(_ => transactionChainService.observeTransaction(tx.transaction.src.address, tx.transaction.hash))
      .void
      .flatTap(_ => Sync[F].delay(dao.metrics.incrementMetric("transactionAccepted")))
      .flatTap(_ => Logger[F].debug(s"Accepting transaction=${tx.hash}"))

  override def pullForConsensus(maxCount: Int): F[List[TransactionCacheData]] =
    count(status = ConsensusStatus.Pending).flatMap {
      case 0L => createDummyTransactions(1)
      case _  => super.pullForConsensus(maxCount)
    }

  def createDummyTransactions(count: Int): F[List[TransactionCacheData]] =
    List
      .fill(count)(
        TransactionService
          .createDummyTransaction(dao.selfAddressStr, KeyUtils.makeKeyPair().getPublic.toId.address, dao.keyPair)(
            transactionChainService
          )
          .map(TransactionCacheData(_))
      )
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
    TransactionService
      .createTransaction(src, dst, amount, keyPair, normalized, dummy)(transactionChainService)

}

object TransactionService {

  def createTransaction[F[_]: Concurrent](
    src: String,
    dst: String,
    amount: Long,
    keyPair: KeyPair,
    normalized: Boolean = true,
    dummy: Boolean = false
  )(transactionChainService: TransactionChainService[F])(implicit hashGenerator: HashGenerator): F[Transaction] = {
    val amountToUse = if (normalized) amount * Schema.NormalizationFactor else amount

    val txData = TransactionEdgeData(amount = amountToUse)(hashGenerator)

    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )(hashGenerator)

    val soe = signedObservationEdge(oe)(keyPair, hashGenerator)

    transactionChainService.setLastTransaction(Edge(oe, soe, txData), dummy)
  }

  def createDummyTransaction[F[_]: Concurrent](src: String, dst: String, keyPair: KeyPair)(
    transactionChainService: TransactionChainService[F]
  )(implicit hashGenerator: HashGenerator): F[Transaction] =
    createTransaction[F](src, dst, 0L, keyPair, normalized = false, dummy = true)(transactionChainService)
}
