package org.constellation.rollback

import cats.implicits._
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives.{CheckpointBlock, Transaction}

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

  def spend(transactions: Seq[Transaction]): AccountBalances =
    transactions.groupBy(_.src.address).mapValues(-_.map(_.amount).sum)

  def received(transactions: Seq[Transaction]): AccountBalances =
    transactions.groupBy(_.dst.address).mapValues(_.map(_.amount).sum)
}
