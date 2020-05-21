package org.constellation.infrastructure.endpoints

import cats.data.Validated.{Invalid, Valid}
import cats.effect.Concurrent
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.generic.auto._
import io.circe.syntax._
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.TransactionService
import org.constellation.primitives
import org.constellation.primitives.Transaction
import org.constellation.util.Metrics
import org.http4s.{HttpRoutes, _}
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

class TransactionEndpoints[F[_]](implicit F: Concurrent[F]) extends Http4sDsl[F] {

  val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  implicit val txDecoder: EntityDecoder[F, Transaction] = jsonOf

  def publicEndpoints(
    transactionService: TransactionService[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ) =
    getTransactionEndpoint(transactionService) <+>
      processTransactionEndpoint(transactionService, checkpointBlockValidator) <+>
      getLastTransactionRefEndpoint(transactionService)

  private def getBatchEndpoint(metrics: Metrics, transactionService: TransactionService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ POST -> Root / "batch" / "transactions" =>
        (for {
          hashes <- req.decodeJson[List[String]]
          _ <- metrics.incrementMetricAsync[F](Metrics.batchTransactionsEndpoint)
          txs <- hashes.traverse(hash => transactionService.lookup(hash).map((hash, _))).map(_.filter(_._2.isDefined))
        } yield txs.asJson).flatMap(Ok(_))
    }

  private def processTransactionEndpoint(
    transactionService: TransactionService[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case req @ POST -> Root / "transaction" =>
        for {
          tx <- req.as[Transaction]
          _ <- logger.info(s"Received transaction hash=${tx.hash}")
          validation <- checkpointBlockValidator.singleTransactionValidation(tx)
          response <- validation match {
            case Invalid(e) => BadRequest(e.toString.asJson)
            case Valid(tx) =>
              transactionService.put(primitives.TransactionCacheData(tx)).map(_.hash.asJson).flatMap(Ok(_))
          }
          _ <- if (validation.isValid) {
            logger.info(s"Received transaction hash=${tx.hash} is valid")
          } else {
            logger.info(
              s"Received transaction hash=${tx.hash} is invalid, cause: ${validation.leftMap(_.map(_.errorMessage))}"
            )
          }
        } yield response
    }

  def peerEndpoints(transactionService: TransactionService[F], metrics: Metrics) =
    getTransactionEndpoint(transactionService) <+>
      getBatchEndpoint(metrics, transactionService) <+>
      getLastTransactionRefEndpoint(transactionService)

  private def getTransactionEndpoint(transactionService: TransactionService[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "transaction" / hash =>
      transactionService.lookup(hash).map(_.asJson).flatMap(Ok(_))
  }

  private def getLastTransactionRefEndpoint(transactionService: TransactionService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "transaction" / "last-ref" / address =>
        transactionService.transactionChainService.getLastAcceptedTransactionRef(address).map(_.asJson).flatMap(Ok(_))
    }
}

object TransactionEndpoints {

  def publicEndpoints[F[_]: Concurrent](
    transactionService: TransactionService[F],
    checkpointBlockValidator: CheckpointBlockValidator[F]
  ): HttpRoutes[F] =
    new TransactionEndpoints[F]().publicEndpoints(transactionService, checkpointBlockValidator)

  def peerEndpoints[F[_]: Concurrent](
    transactionService: TransactionService[F],
    metrics: Metrics
  ): HttpRoutes[F] =
    new TransactionEndpoints[F]().peerEndpoints(transactionService, metrics)
}
