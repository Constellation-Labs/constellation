package org.constellation.util

import constellation._

// Toy example for anti-spam request throttling

/** Documentation. */
trait POWExt {

  /** Documentation. */
  def proofOfWork(input: String, difficulty: Option[Int]): String = {
    var done = false
    var count = 0L
    while (!done) {
      count += 1
      if (verifyPOW(input, count.toString, difficulty)) {
        done = true
      }
    }
    count.toString
  }

  /** Documentation. */
  def hashNonce(input: String, nonce: String): String = {
    val strNonced = input + nonce
    strNonced.sha256.sha256
  }

  /** Documentation. */
  def verifyPOW(input: String, nonce: String, difficulty: Option[Int]): Boolean = {
    val sha = hashNonce(input, nonce)
    difficulty.exists{d => sha.startsWith("0"*d)}
  }

}

/** Documentation. */
object POW extends POWExt

