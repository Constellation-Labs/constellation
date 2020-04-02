package org.constellation.infrastructure.p2p

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.{
  BuildInfoClientAlgebra,
  CheckpointClientAlgebra,
  ClusterClientAlgebra,
  ConsensusClientAlgebra,
  MetricsClientAlgebra,
  NodeMetadataClientAlgebra,
  ObservationClientAlgebra,
  SignClientAlgebra,
  SnapshotClientAlgebra,
  SoeClientAlgebra,
  TipsClientAlgebra,
  TransactionClientAlgebra
}
import org.constellation.infrastructure.p2p.client.{
  BuildInfoClientInterpreter,
  CheckpointClientInterpreter,
  ClusterClientInterpreter,
  ConsensusClientInterpreter,
  MetricsClientInterpreter,
  NodeMetadataClientInterpreter,
  ObservationClientInterpreter,
  SignClientInterpreter,
  SnapshotClientInterpreter,
  SoeClientInterpreter,
  TipsClientInterpreter,
  TransactionClientInterpreter
}
import org.http4s.client.Client

class ClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F]) {
  val buildInfo: BuildInfoClientAlgebra[F] = BuildInfoClientInterpreter[F](client)
  val checkpoint: CheckpointClientAlgebra[F] = CheckpointClientInterpreter[F](client)
  val cluster: ClusterClientAlgebra[F] = ClusterClientInterpreter[F](client)
  val consensus: ConsensusClientAlgebra[F] = ConsensusClientInterpreter[F](client)
  val metrics: MetricsClientAlgebra[F] = MetricsClientInterpreter[F](client)
  val nodeMetadata: NodeMetadataClientAlgebra[F] = NodeMetadataClientInterpreter[F](client)
  val observation: ObservationClientAlgebra[F] = ObservationClientInterpreter[F](client)
  val sign: SignClientAlgebra[F] = SignClientInterpreter[F](client)
  val snapshot: SnapshotClientAlgebra[F] = SnapshotClientInterpreter[F](client)
  val soe: SoeClientAlgebra[F] = SoeClientInterpreter[F](client)
  val tips: TipsClientAlgebra[F] = TipsClientInterpreter[F](client)
  val transaction: TransactionClientAlgebra[F] = TransactionClientInterpreter[F](client)
}

object ClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): ClientInterpreter[F] =
    new ClientInterpreter[F](client)
}
