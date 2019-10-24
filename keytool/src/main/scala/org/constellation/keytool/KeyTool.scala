package org.constellation.keytool

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import org.constellation.BuildInfo
import scopt.OParser

object KeyTool extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      keyStore <- KeyStoreUtils.keyPairToStorePath[IO](
        path = cliParams.keystore,
        alias = cliParams.alias,
        storePassword = cliParams.storepass,
        keyPassword = cliParams.keypass
      )
    } yield keyStore
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, CliConfig] = {
    val builder = OParser.builder[CliConfig]
    import builder._

    /**
      * Follows API parts of https://docs.oracle.com/javase/6/docs/technotes/tools/solaris/keytool.html
      */
    val cliParser = {
      OParser.sequence(
        programName("cl-keytool"),
        head("cl-keytool", BuildInfo.version),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").required
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").required
          .action((x, c) => c.copy(keypass = x.toCharArray))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }
}
