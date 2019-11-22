package org.constellation.util
import java.io.FileInputStream
import java.security.{KeyPair, KeyStore, PrivateKey, PublicKey}

import better.files.File
import cats.effect.IO
import org.constellation.keytool.{KeyStoreUtils, KeyTool, KeyUtils}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class KeyToolTest extends FlatSpec with Matchers with ScalaFutures {
  val keystorePath = "src/test/resources/wallet-client-test-kp.p12"
  val alias = "alias"
  val storepass = "storepass"
  val keypass = "keypass"
  val privateKeyStr = "MIGNAgEAMBAGByqGSM49AgEGBSuBBAAKBHYwdAIBAQQgnav+6JPbFl7APXykQLLaOP4OJbS0pP+D+zGKPEBatfigBwYFK4EEAAqhRANCAATAvvwlwyfMwcz5sebY2OVwXo+CFEC9lT/83Cf/o70KSHpAECl5yrfJsAVo5Y9HIAPLqUgpFG8bD5jEvvXj6U7V"
  val pubKeyStr = "MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwL78JcMnzMHM+bHm2NjlcF6PghRAvZU//Nwn/6O9Ckh6QBApecq3ybAFaOWPRyADy6lIKRRvGw+YxL714+lO1Q=="

  val keyToolArgs = List(
    s"--keystore=${keystorePath}",
    s"--alias=${alias}",
    s"--storepass=${storepass}",
    s"--keypass=${keypass}"
  )

  //todo execution with other tests causes java.lang.SecurityException: JCE cannot authenticate the provider SC
  ignore should "KeyTool create new keypair and save to disk" in {
    val genKeyLoop = for {//TODO set val like in WalletClientTest val keyTool = KeyTool
      kp <- KeyTool.run(keyToolArgs)
    } yield kp
    genKeyLoop.unsafeRunSync()
    assert(File(keystorePath).nonEmpty)
    //tod and      newTxF.map(_ => assert(File(storePath).nonEmpty))
  }

  "KeyStoreUtils" should "load keypair successfully" in {
    val loadKp = for {
      lkp <- KeyStoreUtils.keyPairFromStorePath[IO](keystorePath, alias, storepass.toCharArray, keypass.toCharArray)
    } yield lkp
    val loadedKp = loadKp.value.unsafeRunSync()
    val kp = loadedKp.right.get
    assert(kp.getPublic.isInstanceOf[PublicKey] && kp.getPrivate.isInstanceOf[PrivateKey])
  }

  "keyPairFromPemStr" should "generate keypair from PEM strings" in {
    assert(KeyUtils.keyPairFromPemStr(privateKeyStr, pubKeyStr).isInstanceOf[KeyPair])
  }
}
