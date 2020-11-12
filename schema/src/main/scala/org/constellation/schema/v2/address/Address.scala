package org.constellation.schema.v2.address

import org.constellation.schema.ProtoAutoCodecs
import org.constellation.schema.v2.signature.Signable

case class Address(address: String) extends Signable {

  override def hash: String = address
}

object Address extends ProtoAutoCodecs[org.constellation.schema.proto.address.Address, Address] {
  val cmp = org.constellation.schema.proto.address.Address
}
