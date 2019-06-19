package org.constellation.storage.transactions

object TransactionStatus extends Enumeration {
  type TransactionStatus = Value
  val Pending, Arbitrary, InConsensus, Accepted, Unknown = Value
}
