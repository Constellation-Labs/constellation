package org.constellation.hufflepuff

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.security.{KeyPair, SecureRandom}

import akka.actor.Actor.Receive
import org.constellation.wallet.KeyUtils.makeKeyPair
import org.constellation.blockchain.BlockData

/**
  * Interface for an actor that wishes to perform cryptographic consensus
  */
trait HoneyBadger {

  /**
    * Need to implement as a primitive
    */
  def atomicBroadcast: Unit

  /**
    *
    */
  def reliableBroadcast: Unit

  /**
    * Byzantine consensus from Ben-Or et al. Uses binaryAgreement.
    */
  def benOr: Unit

  /**
    * Figure 4 in HBFT.
    */
  def aCS: Unit = {
    reliableBroadcast
    benOr
  }

  /**
    * Moustefaoui et al. [42], HBFT, based on a cryptographic common coin.
    */
  def binaryAgreement: Unit = {}

  /**
    *
    */
  def receive: Receive = {
    case _ =>
  }
}

/**
  *
  * @param share
  */
case class EncryptionShare(share: Byte)

/**
  * Pure functions
  */
object HoneyBadger {

  /**
    * We may need to implement our own version of Baek and Zheng's threshold encryption,
    * 'Baek and Y. Zheng. Simple and efficient thresholdcryptosystem from the gap diffie-hellman group'
    * Honeybadger: "For threshold encryption of transactions,we use Baek and Zheng’s scheme [7] to encrypt a 256-bit
    * ephemeralkey, followed by AES-256 in CBC mode over the actual payload."
    */
  def thresholdEncryptionSetup(prevBlock: BlockData): KeyPair = {
    /*
    Assuming this is stable, we need a seed to the random number generator that is deterministic across delegates.
    The previous block can serve this purpose from Honeybadger: "TPKE.Setup(executedby a dealer)", so the prev block becomes dealer
     */
    val seed = serialize(prevBlock)
    val keyPair = makeKeyPair(new SecureRandom(seed))

  }

  /**
    * Helper to thresholdEncryptionSetup
    * @param value Any: prevBlock
    * @return Array[Byte]: Byte array of Block, used as seed for SecureRandom in thresholdEncryptionSetup
    */
  def serialize(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  /**
    *
    */
  def encryptShare(buffer: Seq[BlockData], fraction: Double = 1.0): Unit = {
    import java.security.Security
    import java.security.{SecureRandom, _}

    val enc = Security
  }

  /**
    *
    */
  def decryptShare: Unit = {}

  /**
    *
    */
  def decrypt: Unit = {}

}