package me.example.domain

import me.example.domain.UserEvents.{UserCreated, UserEvent, UserUpdated}

import me.milan.domain.Aggregator

case object UserAggregator extends Aggregator[User, UserEvent] {

  override def process(
                        previous: Option[User],
                        event: UserEvent
                      ): User = event match {
    case UserCreated(key, name) =>
      User(key, name, "created")
    case UserUpdated(key, name) =>
      User(key, name, "updated")
  }
}
