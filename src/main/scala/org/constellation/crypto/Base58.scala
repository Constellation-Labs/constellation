package org.constellation.crypto

import java.math.BigInteger
import scala.annotation.tailrec

/** Base58 conversion tools.
  *
  * @see [[https://github.com/ACINQ/bitcoin-lib/blob/master/src/main/scala/fr/acinq/bitcoin/Base58.scala github.com/ACINQ/bitcoin-lib]]
  */
object Base58 {

  val alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
  // char -> value // tmp comment
  val map: Map[Char, Int] = alphabet.zipWithIndex.toMap

  /** Ecodes byte array.
    *
    * @param input ... binary data
    * @return The base-58 representation of input
    */
  def encode(input: Seq[Byte]): String = {
    if (input.isEmpty) ""
    else {
      val big = new BigInteger(1, input.toArray)
      val builder = new StringBuilder

      // doc
      @tailrec
      def encode1(current: BigInteger): Unit = current match {
        case BigInteger.ZERO => ()
        case _ =>
          val Array(x, remainder) = current.divideAndRemainder(BigInteger.valueOf(58L))
          builder.append(alphabet.charAt(remainder.intValue))
          encode1(x)
      }

      encode1(big)
      input.takeWhile(_ == 0).map(_ => builder.append(alphabet.charAt(0)))
      builder.toString().reverse
    }
  }

  /** Decodes byte array.
    *
    * @param input ... base-58 encoded data
    * @return the decoded data
    */
  def decode(input: String): Array[Byte] = {
    val zeroes = input.takeWhile(_ == '1').map(_ => 0: Byte).toArray
    val trim = input.dropWhile(_ == '1').toList
    val decoded = trim.foldLeft(BigInteger.ZERO)((a, b) => a.multiply(BigInteger.valueOf(58L)).add(BigInteger.valueOf(map(b))))
    if (trim.isEmpty) zeroes else zeroes ++ decoded.toByteArray.dropWhile(_ == 0) // BigInteger.toByteArray may add a leading 0x00
  }

} // end Base58 object
