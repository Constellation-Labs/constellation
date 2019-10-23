package org.constellation.crypto.keytool

import cats.data.EitherT
import cats.effect.{ExitCode, IO, Sync}
import org.constellation.BuildInfo
import org.constellation.crypto.KeyStoreUtils

object KeyTool {

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      envParams <- loadEnvParams[IO]
      keyStore <- KeyStoreUtils.keyPairToStorePath[IO](
        path = cliParams.path,
        alias = cliParams.alias,
        storePassword = envParams.storepass,
        keyPassword = envParams.keypass
      )

    } yield keyStore
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, CliConfig] = {
    import scopt.OParser
    val builder = OParser.builder[CliConfig]

    val cliParser = {
      import builder._
      OParser.sequence(
        programName("cl-keytool"),
        head("cl-keytool", BuildInfo.version),
        opt[String]("path")
          .action((x, c) => c.copy(path = x)),
        opt[String]("alias")
          .action((x, c) => c.copy(alias = x))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, CliConfig()).toRight(new RuntimeException("CLI params are missing"))
    }
  }

  def loadEnvParams[F[_]: Sync]: EitherT[F, Throwable, EnvConfig] =
    EitherT.fromEither[F] {
      for {
        storepass <- sys.env.get("CL_STOREPASS").toRight(new RuntimeException("CL_STOREPASS is missing in environment"))
        keypass <- sys.env.get("CL_KEYPASS").toRight(new RuntimeException("CL_KEYPASS is missing in environment"))
      } yield EnvConfig(storepass = storepass.toCharArray, keypass = keypass.toCharArray)
    }
}

case class CliConfig(
  path: String = null,
  alias: String = null
)

case class EnvConfig(
  storepass: Array[Char],
  keypass: Array[Char]
)
