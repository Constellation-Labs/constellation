package org.constellation
import cats.effect.IO
import org.constellation.p2p.PeerData
import org.constellation.primitives.Schema
import org.constellation.primitives.Schema.{Id, NodeState}
import org.constellation.util.{APIClient, HostPort, Metrics}
import org.mockito.IdiomaticMockito

object TestHelpers extends IdiomaticMockito {

  def prepareRealDao(facilitators: Map[Schema.Id, PeerData] = prepareFacilitators()): DAO = {
    val dao: DAO = new DAO {
      override def readyPeers: IO[
        Map[Id, PeerData]
      ] = IO.pure(facilitators)
    }
    dao.initialize()
    dao.metrics = new Metrics()(dao)
    dao.cluster.setNodeState(NodeState.Ready).unsafeRunSync
    dao
  }

  def prepareFacilitators(): Map[Schema.Id, PeerData] = {

    val facilitatorId1 = Schema.Id("b")
    val peerData1: PeerData = mock[PeerData]
    peerData1.peerMetadata shouldReturn mock[PeerMetadata]
    peerData1.peerMetadata.id shouldReturn facilitatorId1
    peerData1.notification shouldReturn Seq()
    peerData1.client shouldReturn mock[APIClient]
    peerData1.client.hostPortForLogging shouldReturn HostPort("http://b", 9000)

    Map(facilitatorId1 -> peerData1)
  }
}
