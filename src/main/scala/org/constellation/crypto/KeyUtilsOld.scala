package org.constellation.crypto

import java.security.KeyPair

import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.constellation.keytool.KeyUtils

// TODO: Remove whole KeyUtilsOld after switching to KeyStore
object KeyUtilsOld extends StrictLogging {

  // TODO: Update to use encrypted wallet see below
  def loadDefaultKeyPair(): KeyPair = {
    import constellation._
    val keyPairFile = File(".dag/key")
    val keyPair: KeyPair =
      if (keyPairFile.notExists) {
        logger.warn(
          s"Key pair not found in $keyPairFile - Generating new key pair"
        )
        val kp = KeyUtils.makeKeyPair()
        keyPairFile.write(kp.json)
        kp
      } else {
        try {
          keyPairFile.lines.mkString.x[KeyPair]
        } catch {
          case e: Exception =>
            logger.error(
              s"Keypair stored in $keyPairFile is invalid. Please delete it and rerun to create a new one.",
              e
            )
            throw e
        }
      }
    keyPair
  }

}
