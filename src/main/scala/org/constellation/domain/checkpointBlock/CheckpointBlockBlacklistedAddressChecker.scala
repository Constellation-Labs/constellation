package org.constellation.domain.checkpointBlock

import cats.effect.{Concurrent, Sync}
import cats.syntax.all._
import org.constellation.domain.blacklist.BlacklistedAddresses
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.transaction.Transaction

object CheckpointBlockBlacklistedAddressChecker {

  def check[F[_]: Concurrent](cb: CheckpointBlock)(
    blacklistedAddresses: BlacklistedAddresses[F]
  ): F[List[Transaction]] =
    cb.transactions.toList
      .pure[F]
      .flatMap(_.traverse(isFromBlockedAddress(_)(blacklistedAddresses)))
      .map(_.flatten)

  private def isFromBlockedAddress[F[_]: Concurrent](tx: Transaction)(
    blacklistedAddresses: BlacklistedAddresses[F]
  ): F[Option[Transaction]] =
    blacklistedAddresses
      .contains(tx.src.address)
      .ifM(Sync[F].pure(tx.some), Sync[F].pure(None))
}
