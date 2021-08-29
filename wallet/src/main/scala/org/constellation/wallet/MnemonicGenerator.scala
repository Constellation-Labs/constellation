package org.constellation.wallet

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.StrictLogging
import org.bitcoinj.wallet.DeterministicSeed

import java.nio.charset.Charset
import scala.collection.JavaConverters._

object MnemonicGenerator extends StrictLogging {

  def stringUTF8ToMnemonic(
                            string: String,
                            hashing_offset: Int = 10000,
                            additional_rounds: Int = 0
                          ): List[String] = {
    val bytes_str = string.getBytes(Charset.forName("UTF-8"));
    val bytes = dhashRounds(bytes_str, hashing_offset, additional_rounds)
    val code = new DeterministicSeed(bytes, "", 0L).getMnemonicCode.asScala.toList
    code
  }

  def dhash(bytes: Array[Byte]): Array[Byte] = {
    val bytes2 = Hashing.sha256().hashBytes(bytes).asBytes()
    Hashing.sha256().hashBytes(bytes2).asBytes()
  }

  def dhashRounds(bytes: Array[Byte], hashing_offset: Int = 10000, additional_rounds: Int = 0): Array[Byte] = {
    var out_bytes = dhash(bytes)
    for (_ <- 0 until (additional_rounds + hashing_offset)) {
      out_bytes = dhash(out_bytes)
    }
    out_bytes
  }
}
