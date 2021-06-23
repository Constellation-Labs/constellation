package org.constellation.wallet

import java.security.KeyPair
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.syntax.all._
import org.constellation.schema.serialization.Kryo
import org.constellation.schema.transaction.{LastTransactionRef, Transaction}
import scopt.OParser

import org.http4s._
import org.http4s.dsl.io._
import org.http4s.implicits._

import org.http4s.client.blaze._
import org.http4s.client._
import org.http4s.circe.CirceEntityDecoder._

import scala.concurrent.ExecutionContext.Implicits.global

object Wallet extends IOApp {

  val lbAddress = "http://lb.constellationnetwork.io:9000"

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      keypair <- getKeypair[IO](cliParams)
      _ <- Kryo.init[IO]().attemptT
      _ <- BlazeClientBuilder[IO](global).resource.use { client =>
        runMethod[IO](cliParams, keypair, client).rethrowT
      }.attemptT
    } yield ()
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def runMethod[F[_]](cliParams: CliConfig, keypair: KeyPair, client: Client[F])(
    implicit F: Sync[F]
  ): EitherT[F, Throwable, Unit] =
    cliParams.method match {
      case CliMethod.ShowAddress =>
        displayAddress[F](keypair)
      case CliMethod.CreateTransaction =>
        createTransaction[F](cliParams, keypair, client).flatMap(storeTransaction[F](cliParams, _))
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

  def lastTxRef[F[_]: Sync](client: Client[F], address: String): EitherT[F, Throwable, LastTransactionRef] =
    client.expect[LastTransactionRef](s"$lbAddress/transaction/last-ref/$address").attemptT

  def createTransaction[F[_]](cliParams: CliConfig, keypair: KeyPair, client: Client[F])(
    implicit F: Sync[F]
  ): EitherT[F, Throwable, Transaction] =
    for {
      prevTx <- KeyStoreUtils
        .readFromFileStream[F, Option[Transaction]](cliParams.prevTxPath, TransactionExt.transactionParser[F])
      src = getAddress(keypair)
      txRef <- lastTxRef(client, src)
      tx <- EitherT.liftF[F, Throwable, Transaction] {
        F.delay {
          TransactionExt.createTransaction(
            prevTx,
            txRef,
            cliParams.forcePrevTx,
            src,
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
      buffer <- EitherT.liftF(TransactionExt.transactionWriter[F](transaction).pure[F])
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
            opt[Unit]("normalized")
              .abbr("n")
              .action((x, c) => c.copy(normalized = true)),
            opt[String]("fee").required
              .valueName("<int>")
              .action((x, c) => c.copy(fee = x.toDouble))
              .validate(x => if (x.toDouble >= 0) success else failure("Value <fee> must be >=0")),
            opt[Boolean]("prevTxForce")
              .action((x, c) => c.copy(forcePrevTx = x)),
            opt[String]("amount").required
              .valueName("<int|long>")
              .abbr("a")
              .action(
                (x, c) =>
                  c.copy(
                    amount =
                      // scopt doesn't allow to look into full list of arguments or prioritize some arguments (like flags)
                      if (args.contains("-n") || args.contains("--normalized")) x.toLong
                      else (x.toDouble * 1e8.toLong).toLong
                  )
              )
              .validate(x => if (x.toLong > 0) success else failure("Value <amount> must be >0"))
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
