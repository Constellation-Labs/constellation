package org.constellation.util.wallet
import better.files.File
import cats.effect.IO
import org.constellation.Fixtures
import org.constellation.keytool.KeyStoreUtils
import org.constellation.primitives.Transaction
import org.scalatest._
import constellation._

class CreateNewTransactionTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val walletClient = CreateNewTransaction
  val accountPath = getClass.getResource("/valid-tx.txt").getPath
  val emptyAccountPath = getClass.getResource("/empty-tx-file.txt").getPath
  val amount = "137.035999084"
  val fee = "0.007297"
  val destination = "receiverAddress"
  val storePath = getClass.getResource("/new-tx.txt").getPath
  val args = List(
    s"--keystore=${Fixtures.keystorePath}",
    s"--alias=${Fixtures.alias}",
    s"--storepass=${Fixtures.storepass}",
    s"--keypass=${Fixtures.keypass}",
    s"--account_path=${accountPath}",
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
        .readFromFileStream[IO, Option[Transaction]](emptyAccountPath, walletClient.transactionParser)
    } yield prevTransactionOp
    val prevTxOp = prevTransactionRes.value.unsafeRunSync().right.get
    assert(prevTxOp.isEmpty)
  }

  "Wallet Client" should "return Some(Transaction) loading prevTx from file" in {
    val prevTransactionRes = for {
      prevTransactionOp <- KeyStoreUtils
        .readFromFileStream[IO, Option[Transaction]](accountPath, walletClient.transactionParser)
    } yield prevTransactionOp
    val prevTxOp = prevTransactionRes.value.unsafeRunSync().right.get
    assert(prevTxOp.isDefined)
    assert(prevTxOp.map(_.lastTxRef).isDefined)
  }

  "Wallet Client" should "create first transaction with kp on disk" in {
    val testArgs = List(
      s"--keystore=${Fixtures.keystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--storepass=${Fixtures.storepass}",
      s"--keypass=${Fixtures.keypass}",
      s"--account_path=${ emptyAccountPath}",
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
      s"--keystore=${Fixtures.keystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--storepass=${Fixtures.storepass}",
      s"--keypass=${Fixtures.keypass}",
      s"--account_path=${accountPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}",
      s"--priv_key_str=${Fixtures.privateKeyStr}",
      s"--pub_key_str=${Fixtures.pubKeyStr}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }

  "Wallet Client" should "create first transaction with private key from cli" in {
    val testArgs = List(
      s"--keystore=${Fixtures.keystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--storepass=${Fixtures.storepass}",
      s"--keypass=${Fixtures.keypass}",
      s"--account_path=${emptyAccountPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}",
      s"--priv_key_str=${Fixtures.privateKeyStr}",
      s"--pub_key_str=${Fixtures.pubKeyStr}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }

  "Wallet Client" should "read env args and process new transaction with previous tx and keypair on disk" in {
    val testArgs = List(
      s"--keystore=${Fixtures.keystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--account_path=${emptyAccountPath}",
      s"--amount=${amount}",
      s"--fee=${fee}",
      s"--destination=${destination}",
      s"--store_path=${storePath}",
      s"--env_args=${Fixtures.envArgs}"
    )

    val newTxLoop = for {
      _ <- walletClient.run(testArgs)
    } yield ()
    val newTxF = newTxLoop.unsafeToFuture()
    newTxF.map(_ => assert(File(storePath).nonEmpty))
  }
}
