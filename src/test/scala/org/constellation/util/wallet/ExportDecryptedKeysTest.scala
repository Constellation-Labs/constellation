package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.constellation.Fixtures
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

class ExportDecryptedKeysTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val exportKeysDecrypted = ExportDecryptedKeys
  val decryptedPrivKeyStorePath = "src/test/resources/decrypted_keystore"
  val decryptedPubKeyStorePath = "src/test/resources/decrypted_keystore.pub"

  val testArgs = List(
    s"--keystore=${Fixtures.keystorePath}",
    s"--alias=${Fixtures.alias}",
    s"--storepass=${Fixtures.storepass}",
    s"--keypass=${Fixtures.keypass}",
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

  "Export Decrypted Keys" should "store decrypted keys in .pem format" in {
    val testArgs = List(
      s"--keystore=${Fixtures.keystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--storepass=${Fixtures.storepass}",
      s"--keypass=${Fixtures.keypass}",
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
