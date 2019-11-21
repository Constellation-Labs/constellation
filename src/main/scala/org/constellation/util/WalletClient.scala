package org.constellation.util

import java.io.FileInputStream

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import org.constellation.domain.transaction.{LastTransactionRef, TransactionService}
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.Transaction
import constellation._
import scopt.OParser


/*
 todo: move to schema project
  */
object WalletClient extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val transactionParser: FileInputStream => IO[Option[Transaction]] =
      KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](ParseExt(_).x[Transaction])
    for {
      cliParams <- loadCliParams[IO](args)
      kp <- KeyStoreUtils.keyPairFromStorePath[IO](cliParams.keystore, cliParams.alias)
      prevTransactionOp <- KeyStoreUtils.readFromFileStream[IO, Option[Transaction]](cliParams.adress_path, transactionParser)
      transactionEdge = TransactionService.createTransactionEdge(//todo, we need to sign on Ordinal and lastTxRef
        KeyUtils.publicKeyToAddressString(kp.getPublic),
        cliParams.destination,
        cliParams.ammount.toDouble.toLong,
        kp
      )
      transaction = Transaction(transactionEdge, prevTransactionOp.map(_.lastTxRef).getOrElse(LastTransactionRef.empty))
    } yield SerExt(transaction).jsonSave(cliParams.adress_path)
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, WalletCliConfig] = {
    val builder = OParser.builder[WalletCliConfig]

    /**
      * Follows API parts of https://docs.oracle.com/javase/6/docs/technotes/tools/solaris/keytool.html
      */
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("wallet-client"),
        // TODO: keytool BuildInfo needs to be generated BEFORE compiling constellation in CircleCI
        //        head("cl-keytool", BuildInfo.version),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").required
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").required
          .action((x, c) => c.copy(keypass = x.toCharArray)),
        opt[String]("account_path").optional()
          .action((x, c) => c.copy(adress_path = x)),
        opt[String]("ammount").optional()
          .action((x, c) => c.copy(keypass = x.toCharArray)),
        opt[String]("fee").optional()
          .action((x, c) => c.copy(keypass = x.toCharArray)),
        opt[String]("receiver").optional()
          .action((x, c) => c.copy(keypass = x.toCharArray))
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
                      adress_path: String = null,
                      ammount: String = null,
                      fee: String = null,
                      destination: String = null
                    )