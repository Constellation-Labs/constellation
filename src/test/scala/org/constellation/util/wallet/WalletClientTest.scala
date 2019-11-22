package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.constellation.keytool.KeyStoreUtils
import org.constellation.primitives.Transaction
import org.scalatest._

class WalletClientTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val walletClient = WalletClient
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

  val args = List(
    s"--keystore=${keystorePath}",
    s"--alias=${alias}",
    s"--storepass=${storepass}",
    s"--keypass=${keypass}",
    s"--address_path=${addressPath}",
    s"--amount=${amount}",
    s"--fee=${fee}",
    s"--destination=${destination}",
    s"--store_path=${storePath}"
  )

  "Wallet Client" should "load CLI params successfully" in {
    val parseCliRes = for {
      cliParams <- walletClient.loadCliParams[IO](args)
    } yield cliParams
    val cli = parseCliRes.value.unsafeRunSync().right.get
    assert(cli.isInstanceOf[WalletCliConfig])
  }

  "Wallet Client" should "return None loading prevTx from empty file" in {
    val prevTransactionRes = for {
      prevTransactionOp <- KeyStoreUtils
        .readFromFileStream[IO, Option[Transaction]](emptyAddressPath, walletClient.transactionParser)
    } yield prevTransactionOp
    val prevTxOp = prevTransactionRes.value.unsafeRunSync().right.get
    assert(prevTxOp.isEmpty)
  }

  "Wallet Client" should "return Some(Transaction) loading prevTx from file" in {
    val prevTransactionRes = for {
      prevTransactionOp <- KeyStoreUtils
        .readFromFileStream[IO, Option[Transaction]](addressPath, walletClient.transactionParser)
    } yield prevTransactionOp
    val prevTxOp = prevTransactionRes.value.unsafeRunSync().right.get
    assert(prevTxOp.isDefined)
    assert(prevTxOp.map(_.lastTxRef).isDefined)
  }

  "Wallet Client" should "create first transaction with kp on disk" in {
    val testArgs = List(
      s"--keystore=${keystorePath}",
      s"--alias=${alias}",
      s"--storepass=${storepass}",
      s"--keypass=${keypass}",
      s"--address_path=${emptyAddressPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }

  "Wallet Client" should "create transaction with key pair from cli and prevTx on disk" in {
    val testArgs = List(
      s"--keystore=${keystorePath}",
      s"--alias=${alias}",
      s"--storepass=${storepass}",
      s"--keypass=${keypass}",
      s"--address_path=${addressPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}",
      s"--priv_key_str=${privateKeyStr}",
      s"--pub_key_str=${pubKeyStr}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }

  "Wallet Client" should "create first transaction with private key from cli" in {
    val testArgs = List(
      s"--keystore=${keystorePath}",
      s"--alias=${alias}",
      s"--storepass=${storepass}",
      s"--keypass=${keypass}",
      s"--address_path=${emptyAddressPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}",
      s"--priv_key_str=${privateKeyStr}",
      s"--pub_key_str=${pubKeyStr}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }
}
