package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

class ExportDecryptedKeysTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val exportKeysDecrypted = ExportDecryptedKeys
  val keystorePath = "src/test/resources/wallet-client-test-kp.p12"
  val alias = "alias"
  val storepass = "storepass"
  val keypass = "keypass"
  val addressPath = "src/test/resources/valid-tx.txt"
  val emptyAddressPath = "src/test/resources/empty-tx-file.txt"
  val amount = "137.035999084"
  val fee = "0.007297"
  val destination = "receiverAddress"
  val storePath = "src/test/resources/new-tx.txt"
  val addressStringStorePath = "src/test/resources/address.txt"
  val decryptedPrivKeyStorePath = "src/test/resources/decrypted_keystore"
  val decryptedPubKeyStorePath = "src/test/resources/decrypted_keystore.pub"

  val privateKeyStr =
    "MIGNAgEAMBAGByqGSM49AgEGBSuBBAAKBHYwdAIBAQQgnav+6JPbFl7APXykQLLaOP4OJbS0pP+D+zGKPEBatfigBwYFK4EEAAqhRANCAATAvvwlwyfMwcz5sebY2OVwXo+CFEC9lT/83Cf/o70KSHpAECl5yrfJsAVo5Y9HIAPLqUgpFG8bD5jEvvXj6U7V"

  val pubKeyStr =
    "MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwL78JcMnzMHM+bHm2NjlcF6PghRAvZU//Nwn/6O9Ckh6QBApecq3ybAFaOWPRyADy6lIKRRvGw+YxL714+lO1Q=="

  val testArgs = List(
    s"--keystore=${keystorePath}",
    s"--alias=${alias}",
    s"--storepass=${storepass}",
    s"--keypass=${keypass}",
    s"--priv_store_path=${decryptedPrivKeyStorePath}",
    s"--pub_store_path=${decryptedPubKeyStorePath}"
  )

  "Export Decrypted Keys" should "load CLI params successfully" in {

    val parseCliRes = for {
      cliParams <- exportKeysDecrypted.loadCliParams[IO](testArgs)
    } yield cliParams
    val cli = parseCliRes.value.unsafeRunSync().right.get
    assert(cli.isInstanceOf[ExportKeysDecryptedConfig])
  }

  "Export Decrypted Keys" should "create address from provided pubkey.pem and store output" in {
    val testArgs = List(
      s"--keystore=${keystorePath}",
      s"--alias=${alias}",
      s"--storepass=${storepass}",
      s"--keypass=${keypass}",
      s"--priv_store_path=${decryptedPrivKeyStorePath}",
      s"--pub_store_path=${decryptedPubKeyStorePath}"
    )

    val exportKeysLoop = for {
      _ <- exportKeysDecrypted.run(testArgs)
    } yield ()
    val generatedAddressF = exportKeysLoop.unsafeToFuture()
    generatedAddressF.map(
      _ =>
        assert(
          File(decryptedPrivKeyStorePath).nonEmpty && File(decryptedPubKeyStorePath).nonEmpty
      )
    )
  }
}
