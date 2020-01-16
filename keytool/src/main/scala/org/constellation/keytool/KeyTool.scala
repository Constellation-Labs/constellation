package org.constellation.keytool

import java.security.{KeyPair, KeyStore}

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import org.constellation.keytool.KeyStoreUtils.loadEnvPasswords
import scopt.OParser

object KeyTool extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      keyStore <- makeKeyPairWith[IO](cliParams)
    } yield keyStore
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def makeKeyPairWith[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, KeyStore] =
    if (Option(cliParams.loadFromEnvArgs).nonEmpty) {
      KeyStoreUtils.keyPairToStorePath[F](
        path = cliParams.keystore,
        alias = cliParams.alias
      )
    } else
      KeyStoreUtils.keyPairToStorePath[F](
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = cliParams.storepass,
        keyPassword = cliParams.keypass
      )

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, CliConfig] = {
    val builder = OParser.builder[CliConfig]

    /**
      * Follows API parts of https://docs.oracle.com/javase/6/docs/technotes/tools/solaris/keytool.html
      */
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("cl-keytool"),
        // TODO: keytool BuildInfo needs to be generated BEFORE compiling constellation in CircleCI
//        head("cl-keytool", BuildInfo.version),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").optional
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").optional
          .action((x, c) => c.copy(keypass = x.toCharArray)),
        opt[String]("env_args").optional
          .action((x, c) => c.copy(loadFromEnvArgs = x))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }
}
