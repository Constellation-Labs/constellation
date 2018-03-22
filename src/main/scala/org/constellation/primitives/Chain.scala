package org.constellation.primitives

import org.constellation.primitives.Block.Block

import scala.collection.mutable.ListBuffer

object Chain {

  case class Chain(chain: ListBuffer[Block] = ListBuffer.empty[Block])

}
