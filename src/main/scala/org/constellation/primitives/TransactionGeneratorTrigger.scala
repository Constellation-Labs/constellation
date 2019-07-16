package org.constellation.primitives

import com.typesafe.scalalogging.StrictLogging
import constellation.futureTryWithTimeoutMetric
import org.constellation.primitives.Schema.NodeState
import org.constellation.util.Periodic
import org.constellation.{ConstellationExecutionContext, DAO}

import scala.concurrent.Future
import scala.util.Try

class TransactionGeneratorTrigger(periodSeconds: Int = 10)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("TransactionGeneratorTrigger", periodSeconds)
    with StrictLogging {

  override def trigger(): Future[Try[Unit]] =
    if (nodeIsReady) {
      futureTryWithTimeoutMetric(
        {
          val start = System.currentTimeMillis()
          dao.transactionGenerator.generate().value.unsafeRunSync
          val elapsed = System.currentTimeMillis() - start
          logger.info(s"Attempt generate transaction took : $elapsed ms")
        },
        "generateTransactionsAttempt"
      )(ConstellationExecutionContext.edge, dao)
    } else Future.successful(Try(()))

  private def nodeIsReady: Boolean = dao.nodeState == NodeState.Ready
}
