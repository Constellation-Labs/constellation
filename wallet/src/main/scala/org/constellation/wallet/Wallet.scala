package org.constellation.wallet

import java.security.KeyPair

import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._
import scopt.OParser

object Wallet extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      keypair <- getKeypair[IO](cliParams)
      _ <- runMethod[IO](cliParams, keypair)
    } yield ()
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def runMethod[F[_]](cliParams: CliConfig, keypair: KeyPair)(implicit F: Sync[F]): EitherT[F, Throwable, Unit] =
    cliParams.method match {
      case CliMethod.ShowAddress =>
        displayAddress[F](keypair)
      case CliMethod.CreateTransaction =>
        createTransaction[F](cliParams, keypair).flatMap(storeTransaction[F](cliParams, _))
      case CliMethod.ShowId =>
        displayId[F](keypair)
      case CliMethod.ShowPublicKey =>
        displayPublicKey[F](keypair)
      case _ =>
        EitherT.leftT[F, Unit](new RuntimeException("Unknown command"))
    }

  def getKeypair[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, KeyPair] =
    if (Option(cliParams.loadFromEnvArgs).nonEmpty) {
      KeyStoreUtils.keyPairFromStorePath[F](cliParams.keystore, cliParams.alias)
    } else {
      KeyStoreUtils.keyPairFromStorePath[F](cliParams.keystore, cliParams.alias, cliParams.storepass, cliParams.keypass)
    }

  def getAddress(keypair: KeyPair): String =
    KeyUtils.publicKeyToAddressString(keypair.getPublic)

  def displayAddress[F[_]](keypair: KeyPair)(implicit F: Sync[F]): EitherT[F, Throwable, Unit] =
    EitherT.liftF[F, Throwable, Unit] { F.delay { println(getAddress(keypair)) } }

  def displayId[F[_]](keypair: KeyPair)(implicit F: Sync[F]): EitherT[F, Throwable, Unit] =
    EitherT.liftF[F, Throwable, Unit] { F.delay { println(KeyUtils.publicKeyToHex(keypair.getPublic)) } }

  def displayPublicKey[F[_]](keypair: KeyPair)(implicit F: Sync[F]): EitherT[F, Throwable, Unit] =
    EitherT.liftF[F, Throwable, Unit] { F.delay { println(keypair.getPublic) } }

  def createTransaction[F[_]](cliParams: CliConfig, keypair: KeyPair)(
    implicit F: Sync[F]
  ): EitherT[F, Throwable, Transaction] =
    for {
      prevTx <- KeyStoreUtils
        .readFromFileStream[F, Option[Transaction]](cliParams.prevTxPath, Transaction.transactionParser[F])
      tx <- EitherT.liftF[F, Throwable, Transaction] {
        F.delay {
          Transaction.createTransaction(
            prevTx,
            getAddress(keypair),
            cliParams.destination,
            cliParams.amount,
            keypair,
            if (cliParams.fee > 0) cliParams.fee.some else none[Double]
          )
        }
      }
    } yield tx

  def storeTransaction[F[_]](cliParams: CliConfig, transaction: Transaction)(
    implicit F: Sync[F]
  ): EitherT[F, Throwable, Unit] =
    for {
      buffer <- EitherT.liftF(Transaction.transactionWriter[F](transaction).pure[F])
      _ <- KeyStoreUtils.storeWithFileStream[F](cliParams.txPath, buffer)
    } yield ()

  def loadCliParams[F[_]: Sync](args: List[String]): EitherT[F, Throwable, CliConfig] = {
    val builder = OParser.builder[CliConfig]

    val cliParser = {
      import builder._
      OParser.sequence(
        programName("cl-wallet"),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").optional
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").optional
          .action((x, c) => c.copy(keypass = x.toCharArray)),
        opt[String]("env_args").optional
          .abbr("e")
          .action((x, c) => c.copy(loadFromEnvArgs = x)),
        cmd("create-transaction")
          .action((_, c) => c.copy(method = CliMethod.CreateTransaction))
          .text("create-transaction")
          .children(
            opt[String]("destination").required
              .valueName("<address>")
              .abbr("d")
              .action((x, c) => c.copy(destination = x)),
            opt[String]("prevTx").required
              .valueName("<file>")
              .abbr("p")
              .action((x, c) => c.copy(prevTxPath = x)),
            opt[String]("txFile").required
              .valueName("<file>")
              .abbr("f")
              .action((x, c) => c.copy(txPath = x)),
            opt[String]("fee").required
              .valueName("<int>")
              .action((x, c) => c.copy(fee = x.toDouble))
              .validate(x => if (x.toDouble >= 0) success else failure("Value <fee> must be >=0")),
            opt[String]("amount").required
              .valueName("<int>")
              .abbr("a")
              .action((x, c) => c.copy(amount = x.toDouble))
              .validate(x => if (x.toDouble > 0) success else failure("Value <amount> must be >0"))
          ),
        cmd("show-address")
          .action((_, c) => c.copy(method = CliMethod.ShowAddress))
          .text("show-address"),
        cmd("show-id")
          .action((_, c) => c.copy(method = CliMethod.ShowId))
          .text("show-id"),
        cmd("show-public-key")
          .action((_, c) => c.copy(method = CliMethod.ShowPublicKey))
          .text("show-public-key")
      )
    }

    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }

}
