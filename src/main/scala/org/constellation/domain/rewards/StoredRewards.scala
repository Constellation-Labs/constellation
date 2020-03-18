package org.constellation.domain.rewards

import atb.trustmodel.{EigenTrust => EigenTrustJ}
import org.constellation.rewards.EigenTrustAgents

case class StoredRewards(agents: EigenTrustAgents, model: EigenTrustJ)
