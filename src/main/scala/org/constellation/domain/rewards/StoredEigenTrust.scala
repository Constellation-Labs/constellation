package org.constellation.domain.rewards

import org.constellation.rewards.EigenTrustAgents
import atb.trustmodel.{EigenTrust => EigenTrustJ}

case class StoredEigenTrust(agents: EigenTrustAgents, model: EigenTrustJ)
