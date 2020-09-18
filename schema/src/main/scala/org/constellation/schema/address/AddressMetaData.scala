package org.constellation.schema.address

import org.constellation.schema.Schema.NormalizationFactor
import org.constellation.schema.signature.Signable

case class AddressMetaData(
  address: String,
  balance: Long = 0L,
  lastValidTransactionHash: Option[String] = None,
  txHashPool: Seq[String] = Seq(),
  txHashOverflowPointer: Option[String] = None,
  oneTimeUse: Boolean = false,
  depth: Int = 0
) extends Signable {

  def normalizedBalance: Long = balance / NormalizationFactor
}
