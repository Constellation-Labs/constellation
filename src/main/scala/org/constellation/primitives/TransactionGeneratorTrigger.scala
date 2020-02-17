package org.constellation.primitives

import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.PeriodicIO
import org.constellation.util.Logging._

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
