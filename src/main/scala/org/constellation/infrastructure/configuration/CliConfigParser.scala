package org.constellation.infrastructure.configuration

import cats.effect.Sync
import cats.implicits._
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
        .text("the ip you can be reached from outside"),
      opt[Int]('p', "port")
        .action((x, c) => c.copy(externalPort = x))
        .text("the port you can be reached from outside"),
      opt[String]('f', "path to file with allocation account balances")
        .action((x, c) => c.copy(allocFilePath = x))
        .text("path to file with allocation account balances"),
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
      opt[Unit]('t', "test-mode")
        .action((x, c) => c.copy(testMode = true))
        .text("Run with test settings"),
      help("help").text("prints this usage text"),
      version("version").text(s"Constellation v${BuildInfo.version}"),
      checkConfig(
        c =>
          if (c.externalIp == null ^ c.externalPort == 0) {
            failure("ip and port must either both be set, or neither.")
          } else success
      )
    )
  }

  def parseCliConfig[F[_]: Sync](args: List[String]): F[CliConfig] =
    Sync[F].delay(OParser.parse(parser, args, CliConfig())).flatMap {
      case Some(c) => c.pure[F]
      case _       => new RuntimeException("Invalid set of cli options").raiseError[F, CliConfig]
    }

  def loadSeedsFromConfig[F[_]: Sync](config: Config): F[Seq[HostPort]] =
    config
      .hasPath("seedPeers")
      .pure[F]
      .ifM(
        config.getStringList("seedPeers").pure[F].map {
          _.asScala
            .map(_.split(":"))
            .map(arr => HostPort(arr(0), arr(1).toInt))
        },
        Seq.empty[HostPort].pure[F]
      )
}
