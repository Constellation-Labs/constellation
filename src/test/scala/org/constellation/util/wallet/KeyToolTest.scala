package org.constellation.util.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}

import better.files.File
import cats.effect.IO
import org.constellation.keytool.{KeyStoreUtils, KeyTool, KeyUtils}
import org.scalatest._

class KeyToolTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val keyTool = KeyTool
  val savedKeystorePath = "src/test/resources/wallet-client-test-save-kp.p12"
  val alias = "alias"
  val storepass = "storepass"
  val keypass = "keypass"

  val privateKeyStr =
    "MIGNAgEAMBAGByqGSM49AgEGBSuBBAAKBHYwdAIBAQQgnav+6JPbFl7APXykQLLaOP4OJbS0pP+D+zGKPEBatfigBwYFK4EEAAqhRANCAATAvvwlwyfMwcz5sebY2OVwXo+CFEC9lT/83Cf/o70KSHpAECl5yrfJsAVo5Y9HIAPLqUgpFG8bD5jEvvXj6U7V"

  val pubKeyStr =
    "MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwL78JcMnzMHM+bHm2NjlcF6PghRAvZU//Nwn/6O9Ckh6QBApecq3ybAFaOWPRyADy6lIKRRvGw+YxL714+lO1Q=="

  val keyToolArgs = List(
    s"--keystore=${savedKeystorePath}",
    s"--alias=${alias}",
    s"--storepass=${storepass}",
    s"--keypass=${keypass}"
  )

  "KeyStoreUtils" should "load keypair successfully" in {
    val loadKp =
      for {
        lkp <- KeyStoreUtils
          .keyPairFromStorePath[IO](savedKeystorePath, alias, storepass.toCharArray, keypass.toCharArray)
      } yield lkp
    val kp = loadKp.value.unsafeRunSync().right.get
    assert(kp.getPublic.isInstanceOf[PublicKey] && kp.getPrivate.isInstanceOf[PrivateKey])
  }

  "KeyTool" should "create new keypair and save to disk" in {
    val genKeyLoop = for {
      kp <- keyTool.run(keyToolArgs)
    } yield kp
    val genKeyLoopF = genKeyLoop.unsafeToFuture()
    genKeyLoopF.map(_ => assert(File(savedKeystorePath).nonEmpty))
  }

  "keyPairFromPemStr" should "generate keypair from PEM strings" in {
    assert(KeyUtils.keyPairFromPemStr(privateKeyStr, pubKeyStr).isInstanceOf[KeyPair])
  }
}
