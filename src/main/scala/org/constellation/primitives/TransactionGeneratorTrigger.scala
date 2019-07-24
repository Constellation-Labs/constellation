package org.constellation.primitives

import cats.effect.IO
import cats.implicits._
import org.constellation.DAO
import org.constellation.util.PeriodicIO

import scala.concurrent.duration._

class TransactionGeneratorTrigger(periodSeconds: Int = 10)(implicit dao: DAO)
    extends PeriodicIO("TransactionGeneratorTrigger") {

  override def trigger(): IO[Unit] =
    nodeIsReady.flatMap(isReady => if (isReady) dao.transactionGenerator.generate().value.void else IO.unit)

  private def nodeIsReady: IO[Boolean] = dao.cluster.isNodeReady

  schedule(periodSeconds seconds)
}
