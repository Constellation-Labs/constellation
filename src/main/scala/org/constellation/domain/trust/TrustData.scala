package org.constellation.domain.trust

import org.constellation.domain.schema.Id

case class TrustData(id: Id, view: Map[Id, Double])
