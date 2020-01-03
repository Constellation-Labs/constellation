package org.constellation.p2p

sealed trait JoiningPeerValidationMessage {

  def errorMessage: String
}

case class JoiningPeerHasInsufficientBalance(address: String) extends JoiningPeerValidationMessage {

  def errorMessage: String = s"Joining peer with address=$address has insufficient balance"
}

case class JoiningPeerHasDifferentVersion(address: String) extends JoiningPeerValidationMessage {

  override def errorMessage: String = s"Joining peer with address=$address started from different commit hash"
}

case class JoiningPeerUnavailable(address: String) extends JoiningPeerValidationMessage {

  override def errorMessage: String = s"Cannot get information from peer with address=$address "
}
