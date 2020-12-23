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
import org.constellation.schema.signature.Signed
import org.constellation.schema.snapshot.SnapshotProposal

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
        (classOf[Signed[SnapshotProposal]], 1032),
        (classOf[GossipMessage[Signed[SnapshotProposal]]], 1033),
        (classOf[GossipPath], 1034)
      )
    ) {}
