package org.constellation.schema.address

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class AddressCacheData(
  balance: Long,
  memPoolBalance: Long,
  reputation: Option[Double] = None,
  ancestorBalances: Map[String, Long] = Map(),
  ancestorReputations: Map[String, Long] = Map(),
  //    recentTransactions: Seq[String] = Seq(),
  balanceByLatestSnapshot: Long = 0L,
  rewardsBalance: Long = 0L
) {

  def plus(previous: AddressCacheData): AddressCacheData =
    this.copy(
      ancestorBalances =
        ancestorBalances ++ previous.ancestorBalances
          .filterKeys(k => !ancestorBalances.contains(k)),
      ancestorReputations =
        ancestorReputations ++ previous.ancestorReputations.filterKeys(
          k => !ancestorReputations.contains(k)
        )
      //recentTransactions =
      //  recentTransactions ++ previous.recentTransactions.filter(k => !recentTransactions.contains(k))
    )

}

object AddressCacheData {
  implicit val addressCacheDataEncoder: Encoder[AddressCacheData] = deriveEncoder
  implicit val addressCacheDataDecoder: Decoder[AddressCacheData] = deriveDecoder
}

// Instead of one balance we need a Map from soe hash to balance and reputation
// These values should be removed automatically by eviction
// We can maintain some kind of automatic LRU cache for keeping track of what we want to remove
// override evict method, and clean up data.
// We should also mark a given balance / rep as the 'primary' one.
