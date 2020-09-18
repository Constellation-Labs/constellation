package org.constellation.primitives

import cats.effect.IO
import cats.syntax.all._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.schema.NodeState
import org.constellation.util.Logging._
import org.constellation.util.PeriodicIO

import scala.concurrent.duration._

class TransactionGeneratorTrigger(periodSeconds: Int = 10, dao: DAO) extends PeriodicIO("TransactionGeneratorTrigger") {

  implicit val implicitLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  override def trigger(): IO[Unit] =
    logThread(
      dao.cluster.getNodeState
        .map(NodeState.canGenerateTransactions)
        .ifM(dao.transactionGenerator.generate().value.void, IO.unit),
      "transactionGenerator_trigger"
    )

  schedule(periodSeconds seconds)
}
