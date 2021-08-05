package org.constellation.keytool

import com.typesafe.scalalogging.StrictLogging

import java.nio.charset.Charset
import java.security.spec.ECGenParameterSpec
import java.security.{SecureRandom, _}

object DeterministicKeyGen extends StrictLogging {

  import KeyUtils._

  private val letters: List[Char] = ('a' to 'z').toList
  // A -> 1 ; Z -> 26
  val letterMapping: Map[Char, Int] = letters.zipWithIndex.toMap.mapValues(_ + 1)

  def mapCompoundLetters(seed: String): String = {
    seed.toLowerCase.filter(letters.contains).flatMap(letterMapping.get).mkString("")
  }

  def makeKeyPairsLetterMap(count: Int = 10, seed: String): List[KeyPair] = {
    val seedBytes = BigInt(mapCompoundLetters(seed)).toByteArray
    makeKeyPairs(count, seedBytes)
  }

  def makeKeyPairsUTFBytes(count: Int = 10, seed: String): List[KeyPair] = {
    makeKeyPairs(count, seed.getBytes(Charset.forName("UTF-8")))
  }

  def makeKeyPairs(count: Int = 10, seed: Array[Byte]): List[KeyPair] = {
    logger.debug("Seed bytes {}", seed.size)
    val detSecureRandom = SecureRandom.getInstance("SHA1PRNG")
    detSecureRandom.setSeed(seed)
    List.fill(count) {
      val keyGen: KeyPairGenerator = KeyPairGenerator.getInstance(ECDSA, provider)
      val ecSpec = new ECGenParameterSpec(secp256k)
      keyGen.initialize(ecSpec, detSecureRandom)
      keyGen.generateKeyPair
    }
  }

}