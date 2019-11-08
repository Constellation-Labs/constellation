package org.constellation.schema

trait HashGenerator {

  def hash(anyRef: AnyRef): String
}
