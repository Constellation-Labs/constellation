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
import org.constellation.schema.serialization.ExplicitKryoRegistrar.KryoSerializer.DefaultSerializer

object ConstellationKryoRegistrar
    extends ExplicitKryoRegistrar(
      Set(
        (classOf[DefaultRandomGenerator], 178, DefaultSerializer),
        (classOf[MersenneTwister], 179, DefaultSerializer),
        (classOf[cern.jet.random.Normal], 180, DefaultSerializer),
        (classOf[cern.jet.random.Uniform], 181, DefaultSerializer),
        (classOf[EigenTrustJ], 183, DefaultSerializer),
        (classOf[EigenTrustAgents], 184, DefaultSerializer),
        (classOf[StoredRewards], 185, DefaultSerializer),
        (classOf[BuildInfoJson], 187, DefaultSerializer),
        (classOf[GossipPath], 1032, DefaultSerializer),
        (classOf[GossipMessage[_]], 1033, DefaultSerializer)
      )
    ) {}
