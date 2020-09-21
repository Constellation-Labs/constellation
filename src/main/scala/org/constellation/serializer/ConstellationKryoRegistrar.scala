package org.constellation.serializer

import atb.common.DefaultRandomGenerator
import atb.trustmodel.{EigenTrust => EigenTrustJ}
import cern.jet.random.engine.MersenneTwister
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.IKryoRegistrar
import org.constellation.domain.rewards.StoredRewards
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.rewards.EigenTrustAgents

class ConstellationKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit =
    this.registerClasses(kryo)

  private def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[DefaultRandomGenerator], 178)
    kryo.register(classOf[MersenneTwister], 179)
    kryo.register(classOf[cern.jet.random.Normal], 180)
    kryo.register(classOf[cern.jet.random.Uniform], 181)
    kryo.register(classOf[EigenTrustJ], 183)
    kryo.register(classOf[EigenTrustAgents], 184)
    kryo.register(classOf[StoredRewards], 185)

    kryo.register(classOf[BuildInfoJson], 187)
  }
}
