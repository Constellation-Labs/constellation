package org.constellation.domain.checkpointBlock

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.domain.transaction.TransactionChainService
import org.constellation.primitives.{CheckpointBlock, Genesis, Transaction}
import org.constellation.storage.SOEService

object AwaitingCheckpointBlock {

  def areReferencesAccepted[F[_]: Concurrent](
    txChainService: TransactionChainService[F]
  )(cb: CheckpointBlock): F[Boolean] = {
    val txs = cb.transactions.toList
    areTransactionsAllowedForAcceptance(txs)(txChainService)
  }

  def areParentsSOEAccepted[F[_]: Concurrent](soeService: SOEService[F])(cb: CheckpointBlock): F[Boolean] = {
    val soeHashes = cb.parentSOEHashes.toList.filterNot(_ == Genesis.Coinbase)
    // TODO: should parent's amount be hardcoded?

    soeHashes
      .traverse(soeService.lookup)
      .map(_.flatten)
      .map(_.size == soeHashes.size)
  }

  def hasNoBlacklistedTxs[F[_]: Concurrent](
    cb: CheckpointBlock
  )(blacklistedAddresses: BlacklistedAddresses[F]): F[Boolean] =
    CheckpointBlockBlacklistedAddressChecker.check(cb)(blacklistedAddresses).map(_.isEmpty)

  private def areTransactionsAllowedForAcceptance[F[_]: Concurrent](
    txs: List[Transaction]
  )(txChainService: TransactionChainService[F]): F[Boolean] =
    txs
      .groupBy(_.src.address)
      .mapValues(_.sortBy(_.ordinal))
      .toList
      .pure[F]
      .flatMap { t =>
        t.traverse {
          case (hash, txs) =>
            txChainService
              .getLastAcceptedTransactionRef(hash)
              .map(txs.headOption.map(_.lastTxRef).contains)
        }
      }
      .map(_.forall(_ == true))
}
