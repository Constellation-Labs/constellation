package org.constellation.schema.address

import org.constellation.schema.signature.Signable

case class Address(address: String) extends Signable {

  override def hash: String = address
}
