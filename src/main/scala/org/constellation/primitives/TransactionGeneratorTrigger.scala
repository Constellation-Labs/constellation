package org.constellation.primitives

import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.util.PeriodicIO
import org.constellation.util.Logging._

import scala.concurrent.duration._

class TransactionGeneratorTrigger(periodSeconds: Int = 10)(implicit dao: DAO)
    extends PeriodicIO("TransactionGeneratorTrigger") {

  implicit val implicitLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  override def trigger(): IO[Unit] =
    logThread(
      nodeIsReady.flatMap(isReady => if (isReady) dao.transactionGenerator.generate().value.void else IO.unit),
      "transactionGenerator_trigger"
    )

  private def nodeIsReady: IO[Boolean] = dao.cluster.isNodeReady

  schedule(periodSeconds seconds)
}
