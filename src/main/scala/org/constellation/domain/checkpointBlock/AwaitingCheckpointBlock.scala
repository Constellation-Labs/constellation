package org.constellation.domain.checkpointBlock

import cats.effect.Concurrent
import cats.syntax.all._
import org.constellation.checkpoint.{CheckpointBlockValidator, CheckpointService}
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.genesis.Genesis
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.schema.transaction.Transaction

object AwaitingCheckpointBlock {

  def areReferencesAccepted[F[_]: Concurrent](
    checkpointBlockValidator: CheckpointBlockValidator[F]
  )(cb: CheckpointBlock): F[Boolean] = {
    val txs = cb.transactions.toList
    areTransactionsAllowedForAcceptance(txs)(checkpointBlockValidator)
  }

  def hasNoBlacklistedTxs[F[_]: Concurrent](
    cb: CheckpointBlock
  )(blacklistedAddresses: BlacklistedAddresses[F]): F[Boolean] =
    CheckpointBlockBlacklistedAddressChecker.check(cb)(blacklistedAddresses).map(_.isEmpty)

  private def areTransactionsAllowedForAcceptance[F[_]: Concurrent](
    txs: List[Transaction]
  )(checkpointBlockValidator: CheckpointBlockValidator[F]): F[Boolean] =
    checkpointBlockValidator.validateLastTxRefChain(txs).map(_.isValid)
}
