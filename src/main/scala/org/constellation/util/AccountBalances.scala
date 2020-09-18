package org.constellation.util

import cats.syntax.all._
import org.constellation.consensus.StoredSnapshot
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.transaction.Transaction

object AccountBalances {

  type AccountBalances = Map[String, Long]

  def getGenesisAccountBalancesFrom(checkpointBlock: CheckpointBlock): AccountBalances =
    getGenesisAccountBalances(checkpointBlock.transactions)

  def getAccountBalancesFrom(snapshot: StoredSnapshot): AccountBalances =
    getAccountBalances(snapshot.checkpointCache.map(_.checkpointBlock).flatMap(_.transactions))

  def getAccountBalancesFrom(checkpointBlock: CheckpointBlock): AccountBalances =
    getAccountBalances(checkpointBlock.transactions)

  private def getGenesisAccountBalances(transactions: Seq[Transaction]): AccountBalances =
    received(transactions)

  private def getAccountBalances(transactions: Seq[Transaction]): AccountBalances =
    spend(transactions) |+| received(transactions)

  private def spend(transactions: Seq[Transaction]): AccountBalances =
    transactions.groupBy(_.src.address).mapValues(-_.map(_.amount).sum)

  private def received(transactions: Seq[Transaction]): AccountBalances =
    transactions.groupBy(_.dst.address).mapValues(_.map(_.amount).sum)
}
