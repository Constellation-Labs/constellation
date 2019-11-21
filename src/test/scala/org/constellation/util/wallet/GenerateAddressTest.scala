package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.constellation.Fixtures
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

class GenerateAddressTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val generateAddress = GenerateAddress
  val addressStringStorePath = "src/test/resources/address.txt"

  "Generate Address" should "load CLI params successfully" in {
    val testArgs = List(
      s"--pub_key_str=${Fixtures.pubKeyStr}",
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
      s"--pub_key_str=${Fixtures.pubKeyStr}",
      s"--store_path=${addressStringStorePath}"
    )
    val generateAddressLoop = for {
      _ <- generateAddress.run(testArgs)
    } yield ()
    val generatedAddressF = generateAddressLoop.unsafeToFuture()
    generatedAddressF.map(_ => assert(File(addressStringStorePath).nonEmpty))
  }
}
