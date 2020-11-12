package org.constellation.schema

import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl._
import scalapb.GeneratedMessageCompanion

trait ProtoCodecs[P <: scalapb.GeneratedMessage, D] {
  def fromProto(p: P)(implicit transformer: Transformer[P, D]): D
  def toProto(d: D)(implicit transformer: Transformer[D, P]): P
}

trait SerDeState[P, D] {
  def serialize(a: D)(implicit transformer: Transformer[D, P]): Array[Byte]
  def deserialize(bytes: Array[Byte])(implicit transformer: Transformer[P, D]): D
}

trait ProtoAutoCodecs[P <: scalapb.GeneratedMessage, D] extends ProtoCodecs[P, D] with SerDeState[P, D] {
  val cmp: GeneratedMessageCompanion[P]

  override def fromProto(p: P)(implicit transformer: Transformer[P, D]): D = p.transformInto[D]
  override def toProto(d: D)(implicit transformer: Transformer[D, P]): P = d.transformInto[P]

  override def serialize(a: D)(implicit transformer: Transformer[D, P]): Array[Byte] = toProto(a).toByteArray
  override def deserialize(bytes: Array[Byte])(implicit transformer: Transformer[P, D]): D =
    fromProto(cmp.parseFrom(bytes))
}
