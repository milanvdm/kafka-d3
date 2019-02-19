package me.milan.domain

import java.util.UUID

case class Key(value: String)

object Key {
  def generate: Key = new Key(UUID.randomUUID().toString)
}
