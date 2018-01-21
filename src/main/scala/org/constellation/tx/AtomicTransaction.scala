package org.constellation.tx

import java.nio.ByteBuffer
import java.security.PublicKey

import org.constellation.wallet.KeyUtils.{base64, bytesToPublicKey, fromBase64}
import org.json4s.native.Serialization

object AtomicTransaction {

  def bytesToLong(bytes: Array[Byte]): Long = {
    val buffer = ByteBuffer.allocate(8)
    buffer.put(bytes)
    buffer.flip();//need flip
    buffer.getLong()
  }

  def longToBytes(l: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(0, l)
    buffer.array()
  }

  case class TransactionInputData(
                                   sourcePubKey: PublicKey,
                                   destinationAddress: String,
                                   quantity: Long
                                 ) {
    def encode = EncodedTransaction(
      base64(sourcePubKey.getEncoded),
      destinationAddress,
      base64(longToBytes(quantity))
    )
  }

  case class EncodedTransaction(
                                 sourceAddress: String,
                                 destinationAddress: String,
                                 quantity: String
                               ) {
    def decode = TransactionInputData(
      bytesToPublicKey(fromBase64(sourceAddress)),
      destinationAddress,
      bytesToLong(fromBase64(quantity))
    )
    def ordered = Array(sourceAddress, destinationAddress, quantity)
    def rendered: String = {
      import org.json4s._
      implicit val f: DefaultFormats.type = DefaultFormats
      Serialization.write(ordered)
    }
  }

  def txFromArray(array: Array[String]): EncodedTransaction = {
    val Array(sourceAddress, destinationAddress, quantity) = array
    EncodedTransaction(sourceAddress, destinationAddress, quantity)
  }

  def txFromString(rendered: String): EncodedTransaction = {
    import org.json4s._
    implicit val f: DefaultFormats.type = DefaultFormats
    val array = Serialization.read[Array[String]](rendered)
    txFromArray(array)
  }

}
