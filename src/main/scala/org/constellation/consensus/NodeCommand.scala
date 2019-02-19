package org.constellation.consensus

sealed trait NodeCommand

case object StartBlockCreationRound extends NodeCommand
