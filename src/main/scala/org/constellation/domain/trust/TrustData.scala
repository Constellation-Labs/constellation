package org.constellation.domain.trust

import org.constellation.schema.Id

case class TrustData(view: Map[Id, Double])
case class TrustDataInternal(id: Id, view: Map[Id, Double])
