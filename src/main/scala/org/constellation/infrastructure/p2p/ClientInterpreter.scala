package org.constellation.infrastructure.p2p

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client._
import org.constellation.infrastructure.p2p.client._
import org.constellation.session.SessionTokenService
import org.http4s.client.Client

class ClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) {
  val buildInfo: BuildInfoClientAlgebra[F] = BuildInfoClientInterpreter[F](client, sessionTokenService)
  val checkpoint: CheckpointClientAlgebra[F] = CheckpointClientInterpreter[F](client, sessionTokenService)
  val cluster: ClusterClientAlgebra[F] = ClusterClientInterpreter[F](client, sessionTokenService)
  val consensus: ConsensusClientAlgebra[F] = ConsensusClientInterpreter[F](client, sessionTokenService)
  val healthcheck: HealthCheckClientAlgebra[F] = HealthCheckClientInterpreter[F](client, sessionTokenService)
  val metrics: MetricsClientAlgebra[F] = MetricsClientInterpreter[F](client, sessionTokenService)
  val nodeMetadata: NodeMetadataClientAlgebra[F] = NodeMetadataClientInterpreter[F](client, sessionTokenService)
  val observation: ObservationClientAlgebra[F] = ObservationClientInterpreter[F](client, sessionTokenService)
  val sign: SignClientAlgebra[F] = SignClientInterpreter[F](client)
  val snapshot: SnapshotClientAlgebra[F] = SnapshotClientInterpreter[F](client, sessionTokenService)
  val soe: SoeClientAlgebra[F] = SoeClientInterpreter[F](client, sessionTokenService)
  val tips: TipsClientAlgebra[F] = TipsClientInterpreter[F](client, sessionTokenService)
  val transaction: TransactionClientAlgebra[F] = TransactionClientInterpreter[F](client, sessionTokenService)
}

object ClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): ClientInterpreter[F] =
    new ClientInterpreter[F](client, sessionTokenService)
}
