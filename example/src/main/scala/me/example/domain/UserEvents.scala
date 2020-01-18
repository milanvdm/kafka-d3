package me.example.domain

import me.milan.domain.Key

object UserEvents {

  sealed trait UserEvent extends Product with Serializable
  case class UserCreated(
    id: Key,
    name: String
  ) extends UserEvent
  case class UserUpdated(
    id: Key,
    name: String
  ) extends UserEvent

}
