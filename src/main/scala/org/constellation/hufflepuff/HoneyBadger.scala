package org.constellation.hufflepuff

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
    *
    */
  def receive: Unit

}

/**
  * Pure functions
  */
object HoneyBadger {

  /**
    * Moustefaoui et al. [42], HBFT, based on a cryptographic common coin.
    */
  def binaryAgreement: Unit = {}

  /**
    *
    */
  def thresholdEncryptionSetup: Unit = {}

  /**
    *
    */
  def encryptMsg: Unit = {}

  /**
    *
    */
  def decryptShare: Unit = {}

  /**
    *
    */
  def decrypt: Unit = {}

}