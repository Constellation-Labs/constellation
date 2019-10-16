package org.constellation.domain.transaction

case class LastTransactionRef(
  hash: String,
  ordinal: Long
)

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}
