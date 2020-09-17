package org.constellation.infrastructure.configuration

import better.files.File
import cats.effect.Sync
import cats.syntax.all._
import com.typesafe.config.Config
import org.constellation.BuildInfo
import org.constellation.domain.configuration.CliConfig
import org.constellation.util.HostPort
import scopt.OParser

import scala.collection.JavaConverters._

object CliConfigParser {

  private val parser = {
    val builder = OParser.builder[CliConfig]
    import builder._

    OParser.sequence(
      programName("constellation"),
      head("constellation", BuildInfo.version),
      opt[java.net.InetAddress]("ip")
        .action((x, c) => c.copy(externalIp = x))
        .valueName("<ip address>")
        .text("the ip you can be reached from outside")
        .required,
      opt[Int]('p', "port")
        .action((x, c) => c.copy(externalPort = x))
        .text("the port you can be reached from outside")
        .required,
      opt[String]('f', "alloc")
        .action((x, c) => c.copy(allocFilePath = x))
        .text("path to file with allocation account balances")
        .children(
          opt[Unit]("normalized")
            .action((x, c) => c.copy(allocFileNormalized = true))
            .text("are allocation balances already normalized in file")
        ),
      opt[String]('w', "whitelisting")
        .action((x, c) => c.copy(whitelisting = x))
        .text("path to file with whitelisting")
        .required(),
      opt[Unit]('d', "debug")
        .action((x, c) => c.copy(debug = true))
        .text("run the node in debug mode"),
      opt[Unit]('o', "offline")
        .action((x, c) => c.copy(startOfflineMode = true))
        .text("Start the node in offline mode. Won't connect automatically"),
      opt[Unit]('l', "light")
        .action((x, c) => c.copy(lightNode = true))
        .text("Start a light node, only validates & stores portions of the graph"),
      opt[Unit]('g', "genesis")
        .action((x, c) => c.copy(genesisNode = true))
        .text("Start in single node genesis mode"),
      opt[Unit]('r', "rollback")
        .action((x, c) => c.copy(rollbackNode = true))
        .text("Start in rollback mode")
        .children(
          opt[Long]("height").required
            .valueName("<height>")
            .action((x, c) => c.copy(rollbackHeight = x)),
          opt[String]("hash").required
            .valueName("<hash>")
            .action((x, c) => c.copy(rollbackHash = x))
        ),
      opt[Unit]('t', "test-mode")
        .action((x, c) => c.copy(testMode = true))
        .text("Run with test settings"),
      opt[String]('k', "keystore").required
        .action((x, c) => c.copy(keyStorePath = x))
        .text("Path to keystore file"),
      opt[String]("alias").required
        .action((x, c) => c.copy(alias = x))
        .text("Alias for keypair in provided keystore file"),
      opt[String]("cloud")
        .action((x, c) => c.copy(cloud = x))
        .text("Path to the cloud providers configuration file"),
      help("help").text("prints this usage text"),
      version("version").text(s"Constellation v${BuildInfo.version}"),
      checkConfig(
        c =>
          for {
            _ <- checkConfigOption(
              c.genesisNode && c.rollbackNode,
              "can't start in rollback mode and genesis mode at the same time"
            )
            _ <- checkConfigOption(
              !(c.cloud == null || c.cloud != null && File(c.cloud).exists()),
              "provided cloud providers configuration file does not exist"
            )
          } yield ()
      )
    )
  }

  private def checkConfigOption(test: Boolean, errorMsg: String): Either[String, Unit] =
    Either.cond(!test, (), errorMsg)

  def parseCliConfig[F[_]: Sync](args: List[String]): F[CliConfig] =
    Sync[F].delay(OParser.parse(parser, args, CliConfig())).flatMap {
      case Some(c) => c.pure[F]
      case _       => new RuntimeException("Invalid set of cli options").raiseError[F, CliConfig]
    }

  // TODO: Remove because of the whitelisting
  def loadSeedsFromConfig[F[_]: Sync](config: Config): F[Seq[HostPort]] =
    Sync[F].pure { Seq.empty[HostPort] }
}
