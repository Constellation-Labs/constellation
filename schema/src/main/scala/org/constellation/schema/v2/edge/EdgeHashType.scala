package org.constellation.schema.v2.edge

import enumeratum.{CirceEnum, Enum, EnumEntry}

/** Our basic set of allowed edge hash types */
sealed trait EdgeHashType extends EnumEntry

object EdgeHashType extends Enum[EdgeHashType] with CirceEnum[EdgeHashType] {

  case object AddressHash extends EdgeHashType
  case object CheckpointDataHash extends EdgeHashType
  case object CheckpointHash extends EdgeHashType
  case object TransactionDataHash extends EdgeHashType
  case object TransactionHash extends EdgeHashType
  case object ValidationHash extends EdgeHashType
  case object BundleDataHash extends EdgeHashType
  case object ChannelMessageDataHash extends EdgeHashType

  val values = findValues
}
