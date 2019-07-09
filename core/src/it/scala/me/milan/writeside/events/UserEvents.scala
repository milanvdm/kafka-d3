package me.milan.writeside.events

import me.milan.domain._

object UserEvents {

  val userId: Key = Key.generate
  val userId2: Key = Key.generate

  sealed trait UserEvent
  case class UserCreated(
    id: Key,
    name: String
  ) extends UserEvent
  case class UserUpdated(
    id: Key,
    name: String
  ) extends UserEvent
  case class UserRemoved(id: Key) extends UserEvent

  sealed trait UserState
  case class User(
    id: Key,
    name: String,
    status: String
  ) extends UserState
  case class UserTomb(id: Key) extends UserState with TombStone

  case object UserAggregator extends Aggregator[UserState, UserEvent] {

    override def process(
      previous: Option[UserState],
      event: UserEvent
    ): UserState = event match {
      case UserCreated(key, name) =>
        User(key, name, "created")
      case UserUpdated(key, name) =>
        User(key, name, "updated")
      case UserRemoved(id) =>
        UserTomb(id)
    }
  }
}
