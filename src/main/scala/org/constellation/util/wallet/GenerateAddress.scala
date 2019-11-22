package org.constellation.util.wallet
import java.io.FileOutputStream

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import constellation._
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import scopt.OParser

object GenerateAddress extends IOApp {
  val addressWriter: String => FileOutputStream => IO[Unit] =
    KeyStoreUtils.storeTypeToFileStream[IO, String](SerExt(_).json)

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      address = KeyUtils.publicKeyToAddressString(KeyUtils.pemToPublicKey(cliParams.pubKeyStr))
      _ <- KeyStoreUtils.storeWithFileStream[IO](cliParams.storePath, addressWriter(address))
    } yield address
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, GenerateAddressConfig] = {
    val builder = OParser.builder[GenerateAddressConfig]
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("address-generator"),
        opt[String]("pub_key_str").optional
          .action((x, c) => c.copy(pubKeyStr = x)),
        opt[String]("store_path").required
          .action((x, c) => c.copy(storePath = x)),
      )
    }
    EitherT.fromEither[F] {
      OParser
        .parse(cliParser, args, GenerateAddressConfig())
        .toRight(new RuntimeException("GenerateAddress CLI params are missing"))
    }
  }
}

case class GenerateAddressConfig(
  pubKeyStr: String = null,
  storePath: String = null
)
