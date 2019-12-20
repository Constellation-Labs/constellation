package org.constellation.util.wallet

import java.io._
import java.security._

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import constellation._
import org.constellation.domain.transaction.{LastTransactionRef, TransactionService}
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.Transaction
import scopt.OParser

object CreateNewTransaction extends IOApp {

  /*
  Note: these vals need type annotation to compile
   */
  val transactionParser: FileInputStream => IO[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](ParseExt(_).x[Transaction])

  val transactionWriter: Transaction => FileOutputStream => IO[Unit] =
    KeyStoreUtils.writeTypeToFileStream[IO, Transaction](SerExt(_).json)

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      kp <- loadKeyPairFrom[IO](cliParams)
      prevTransactionOp <- KeyStoreUtils.readFromFileStream[IO, Option[Transaction]](cliParams.accountPath,
                                                                                     transactionParser)
      transactionEdge = TransactionService.createTransactionEdge(
        KeyUtils.publicKeyToAddressString(kp.getPublic),
        cliParams.destination,
        prevTransactionOp.map(tx => LastTransactionRef(tx.hash, tx.ordinal)).getOrElse(LastTransactionRef.empty),
        cliParams.amount.toDouble.toLong,
        kp
      )
      transaction = Transaction(transactionEdge, prevTransactionOp.map(_.lastTxRef).getOrElse(LastTransactionRef.empty))
      transactionWriteBuffer = transactionWriter(transaction)
      _ <- KeyStoreUtils.storeWithFileStream[IO](cliParams.storePath, transactionWriteBuffer)
    } yield transaction
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadKeyPairFrom[F[_]: Sync](cliParams: WalletCliConfig): EitherT[F, Throwable, KeyPair] =
    if (Option(cliParams.loadFromEnvArgs).nonEmpty)
      KeyStoreUtils.keyPairFromStorePath(cliParams.keystore, cliParams.alias)
    else
      Option(cliParams.privateKeyStr)
        .fold(
          KeyStoreUtils
            .keyPairFromStorePath[F](cliParams.keystore, cliParams.alias, cliParams.storepass, cliParams.keypass)
        )(
          privateKeyStr =>
            EitherT
              .rightT(KeyUtils.keyPairFromPemStr(privateKeyStr, cliParams.pubKeyStr))
              .leftMap { _: Exception =>
                new Throwable("Couldn't load KeyPair with PrivateKey provided")
            }
        )

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, WalletCliConfig] = {
    val builder = OParser.builder[WalletCliConfig]
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("wallet-client"),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").optional()
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").optional()
          .action((x, c) => c.copy(keypass = x.toCharArray)).optional(),
        opt[String]("account_path").optional()
          .action((x, c) => c.copy(accountPath = x)),
        opt[String]("amount").required
          .action((x, c) => c.copy(amount = x)),
        opt[String]("fee").required
          .action((x, c) => c.copy(fee = x)),
        opt[String]("destination").required
          .action((x, c) => c.copy(destination = x)),
        opt[String]("store_path").required
          .action((x, c) => c.copy(storePath = x)),
        opt[String]("priv_key_str").optional
          .action((x, c) => c.copy(privateKeyStr = x)),
        opt[String]("pub_key_str").optional
          .action((x, c) => c.copy(pubKeyStr = x)),
        opt[String]("env_args").optional
          .action((x, c) => c.copy(loadFromEnvArgs = x))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, WalletCliConfig()).toRight(new RuntimeException("wallet CLI params are missing"))
    }
  }
}

case class WalletCliConfig(
  keystore: String = null,
  alias: String = null,
  storepass: Array[Char] = null,
  keypass: Array[Char] = null,
  accountPath: String = null,
  amount: String = null,
  fee: String = null,
  destination: String = null,
  storePath: String = null,
  privateKeyStr: String = null,
  pubKeyStr: String = null,
  loadFromEnvArgs: String = null
)
