package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

class GenerateAddressTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val generateAddress = GenerateAddress
  val addressStringStorePath = "src/test/resources/address.txt"

  val pubKeyStr =
    "MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwL78JcMnzMHM+bHm2NjlcF6PghRAvZU//Nwn/6O9Ckh6QBApecq3ybAFaOWPRyADy6lIKRRvGw+YxL714+lO1Q=="

  "Generate Address" should "load CLI params successfully" in {
    val testArgs = List(
      s"--pub_key_str=${pubKeyStr}",
      s"--store_path=${addressStringStorePath}"
    )
    val parseCliRes = for {
      cliParams <- generateAddress.loadCliParams[IO](testArgs)
    } yield cliParams
    val cli = parseCliRes.value.unsafeRunSync().right.get
    assert(cli.isInstanceOf[GenerateAddressConfig])
  }

  "Generate Address" should "create address from provided pubkey.pem and store output" in {
    val testArgs = List(
      s"--pub_key_str=${pubKeyStr}",
      s"--store_path=${addressStringStorePath}"
    )
    val generateAddressLoop = for {
      _ <- generateAddress.run(testArgs)
    } yield ()
    val generatedAddressF = generateAddressLoop.unsafeToFuture()
    generatedAddressF.map(_ => assert(File(addressStringStorePath).nonEmpty))
  }
}
