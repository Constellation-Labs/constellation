package org.constellation.util

import java.io.{FileInputStream, FileOutputStream}
import java.security.PublicKey

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import constellation._
import org.constellation.domain.transaction.{LastTransactionRef, TransactionService}
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.Transaction
import scopt.OParser

/*
 todo: move to schema project
 */
object WalletClient extends IOApp {
  val transactionParser: FileInputStream => IO[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](ParseExt(_).x[Transaction])
  val transactionWriter: Transaction => FileOutputStream => IO[Unit] =
    KeyStoreUtils.storeTypeToFileStream[IO, Transaction](SerExt(_).json)

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      kp <- KeyStoreUtils
        .keyPairFromStorePath[IO](cliParams.keystore, cliParams.alias, cliParams.storepass, cliParams.keypass)
      prevTransactionOp <- KeyStoreUtils.readFromFileStream[IO, Option[Transaction]](cliParams.addressPath,
                                                                                     transactionParser)//todo either here or ln 36
      transactionEdge = TransactionService.createTransactionEdge( //todo, we need to sign on Ordinal + lastTxRef
        KeyUtils.publicKeyToAddressString(kp.getPublic),
        cliParams.destination,
        cliParams.amount.toDouble.toLong,
        kp
      )
      transaction = Transaction(transactionEdge, prevTransactionOp.map(_.lastTxRef).getOrElse(LastTransactionRef.empty))
      transactionWriteBuffer = transactionWriter(transaction)
      _ <- KeyStoreUtils.storeWithFileStream[IO](cliParams.storePath, transactionWriteBuffer)
    } yield transaction
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

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
        opt[String]("storepass").required
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").required
          .action((x, c) => c.copy(keypass = x.toCharArray)) required,
        opt[String]("address_path").required
          .action((x, c) => c.copy(addressPath = x)),
        opt[String]("amount").required
          .action((x, c) => c.copy(amount = x)),
        opt[String]("fee").required
          .action((x, c) => c.copy(fee = x)),
        opt[String]("destination").required
          .action((x, c) => c.copy(destination = x)),
        opt[String]("store_path").required
          .action((x, c) => c.copy(storePath = x))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, WalletCliConfig()).toRight(new RuntimeException("wallet CLI params are missing"))
    }
  }
}

object PublicKeyToAddressString extends App {
  val pubKey: PublicKey = ??? //todo parse string/hex to PublicKey
  KeyUtils.publicKeyToAddressString(pubKey)
  //todo save to file
}

case class WalletCliConfig(
  keystore: String = null,
  alias: String = null,
  storepass: Array[Char] = null,
  keypass: Array[Char] = null,
  addressPath: String = null,
  amount: String = null,
  fee: String = null,
  destination: String = null,
  storePath: String = null
)
