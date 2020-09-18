package org.constellation.schema

import enumeratum.{CirceEnum, Enum, EnumEntry}

sealed trait NodeType extends EnumEntry

object NodeType extends Enum[NodeType] with CirceEnum[NodeType] {
  case object Full extends NodeType
  case object Light extends NodeType

  val values = findValues
}
