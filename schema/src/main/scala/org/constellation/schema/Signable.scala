package org.constellation.schema

trait Signable {

  def hash: String

  def signInput: Array[Byte] = hash.getBytes()

  def short: String = hash.slice(0, 5)
}
