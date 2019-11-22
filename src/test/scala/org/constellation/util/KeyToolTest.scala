package org.constellation.util
import java.io.FileInputStream
import java.security.{KeyPair, KeyStore, PrivateKey, PublicKey}

import better.files.File
import cats.effect.IO
import org.constellation.keytool.{KeyStoreUtils, KeyTool}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class KeyToolTest extends FlatSpec with Matchers with ScalaFutures {
  val keystorePath = "src/test/resources/wallet-client-test-kp.p12"
  val alias = "alias"
  val storepass = "storepass"
  val keypass = "keypass"

  val keyToolArgs = List(
    s"--keystore=${keystorePath}",
    s"--alias=${alias}",
    s"--storepass=${storepass}",
    s"--keypass=${keypass}"
  )

  //todo execution with other tests causes java.lang.SecurityException: JCE cannot authenticate the provider SC
  ignore should "KeyTool create new keypair and save to disk" in {
    val genKeyLoop = for {
      kp <- KeyTool.run(keyToolArgs)
    } yield kp
    genKeyLoop.unsafeRunSync()
    assert(File(keystorePath).nonEmpty)
  }

  "KeyStoreUtils" should "load keypair successfully" in {
    val loadKp = for {
      lkp <- KeyStoreUtils.keyPairFromStorePath[IO](keystorePath, alias, storepass.toCharArray, keypass.toCharArray)
    } yield lkp
    val loadedKp = loadKp.value.unsafeRunSync()
    val kp = loadedKp.right.get
    assert(kp.getPublic.isInstanceOf[PublicKey] && kp.getPrivate.isInstanceOf[PrivateKey])
  }
}
