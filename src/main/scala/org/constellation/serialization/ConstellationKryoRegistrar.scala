package org.constellation.serialization

import atb.common.DefaultRandomGenerator
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import org.constellation.domain.rewards.StoredRewards
import org.constellation.gossip.sampling.GossipPath
import org.constellation.gossip.state.GossipMessage
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.rewards.EigenTrustAgents
import org.constellation.schema.serialization.ExplicitKryoRegistrar
import org.constellation.schema.snapshot.SnapshotProposalPayload

object ConstellationKryoRegistrar
    extends ExplicitKryoRegistrar(
      Set(
        (classOf[DefaultRandomGenerator], 178),
        (classOf[MersenneTwister], 179),
        (classOf[cern.jet.random.Normal], 180),
        (classOf[cern.jet.random.Uniform], 181),
        (classOf[EigenTrustJ], 183),
        (classOf[EigenTrustAgents], 184),
        (classOf[StoredRewards], 185),
        (classOf[BuildInfoJson], 187),
        (classOf[GossipPath], 1032),
        (classOf[GossipMessage[SnapshotProposalPayload]], 1033),
      )
    ) {}
