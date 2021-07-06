package org.constellation.keytool

import java.security.KeyStore

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import org.constellation.keytool.KeyStoreUtils.loadEnvPasswords
import scopt.OParser

object KeyTool extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      _ <- runMethod[IO](cliParams)
    } yield ()
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def runMethod[F[_]](cliParams: CliConfig)(implicit F: Sync[F]): EitherT[F, Throwable, Unit] =
    cliParams.method match {
      case CliMethod.GenerateWallet =>
        generateKeyStoreWithKeyPair[F](cliParams).map(_ => ())
      case CliMethod.MigrateExistingKeyStoreToStorePassOnly =>
        migrateKeyStoreToSinglePassword[F](cliParams).map(_ => ())
      case CliMethod.ExportPrivateKeyHex =>
        exportPrivateKeyAsHex[F](cliParams).map(_ => ())
      case CliMethod.PrintKeyInfo => printKeyInfo(cliParams)
      case _ =>
        EitherT.leftT[F, Unit](new RuntimeException("Unknown command"))
    }

  private def exportPrivateKeyAsHex[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, String] =
    getPasswords(cliParams).flatMap { passwords =>
      KeyStoreUtils.exportPrivateKeyAsHex(
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = passwords.storepass,
        keyPassword = passwords.keypass
      )
    }

  private def migrateKeyStoreToSinglePassword[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, KeyStore] =
    getPasswords(cliParams).flatMap { passwords =>
      KeyStoreUtils.migrateKeyStoreToSinglePassword(
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = passwords.storepass,
        keyPassword = passwords.keypass
      )
    }

  private def generateKeyStoreWithKeyPair[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, KeyStore] =
    getPasswords(cliParams).flatMap { passwords =>
      KeyStoreUtils.generateKeyPairToStorePath(
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = passwords.storepass,
        keyPassword = passwords.keypass
      )
    }

  private def printKeyInfo[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, Unit] =
    getPasswords(cliParams).flatMap { passwords =>
      KeyStoreUtils.printKeyInfo(
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = passwords.storepass,
        keyPassword = passwords.keypass
      )
    }

  private def getPasswords[F[_]: Sync](cliParams: CliConfig): EitherT[F, Throwable, Passwords] =
    if (Option(cliParams.loadFromEnvArgs).nonEmpty) {
      loadEnvPasswords
    } else {
      EitherT.pure(Passwords(storepass = cliParams.storepass, keypass = cliParams.keypass))
    }

  private def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, CliConfig] = {
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
          .action((x, c) => c.copy(loadFromEnvArgs = x)),
        cmd("generate-wallet")
          .action((_, c) => c.copy(method = CliMethod.GenerateWallet))
          .text("Generate wallet (KeyStore with KeyPair inside)"),
        cmd("migrate-to-store-password-only")
          .action((_, c) => c.copy(method = CliMethod.MigrateExistingKeyStoreToStorePassOnly))
          .text("Clone existing KeyStore and set storepass for both store and keypair"),
        cmd("export-private-key-hex")
          .action((_, c) => c.copy(method = CliMethod.ExportPrivateKeyHex))
          .text("Exports PrivateKey in hexadecimal format"),
        cmd("print-key-info")
          .action((_, c) => c.copy(method = CliMethod.PrintKeyInfo))
          .text("Prints PublicKey in hexadecimal format and DAG address to stdout")
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }
}
