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

  def runMethod[F[_]: Sync](cliParams: CliConfig, keypair: KeyPair): EitherT[F, Throwable, Unit] =
    cliParams.method match {
      case CliMethod.ShowAddress =>
        displayAddress[F](keypair)
      case CliMethod.CreateTransaction =>
        EitherT.leftT[F, Unit](new NotImplementedError("Command not implemented"))
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
              .abbr("d")
              .action((x, c) => c),
            opt[String]("prev_tx").required
              .abbr("p")
              .action((x, c) => c),
            opt[String]("fee").required
              .abbr("f")
              .action((x, c) => c),
            opt[String]("amount").required
              .abbr("a")
              .action((x, c) => c)
          ),
        cmd("show-address")
          .action((_, c) => c.copy(method = CliMethod.ShowAddress))
          .text("show-address")
      )
    }

    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }

}
