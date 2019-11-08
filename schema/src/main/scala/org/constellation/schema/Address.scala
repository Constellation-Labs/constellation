package org.constellation.schema

case class Address(address: String) extends Signable {

  override def hash: String = address
}

case class AddressMetaData(
  address: String,
  balance: Long = 0L,
  lastValidTransactionHash: Option[String] = None,
  txHashPool: Seq[String] = Seq(),
  txHashOverflowPointer: Option[String] = None,
  oneTimeUse: Boolean = false,
  depth: Int = 0
)(implicit hashGenerator: HashGenerator)
    extends Signable {

  override def hash: String = hashGenerator.hash(this)

  def normalizedBalance: Long = balance / NormalizationProperties.NORMALIZATION_FACTOR
}
