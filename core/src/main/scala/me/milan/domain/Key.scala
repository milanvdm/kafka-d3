package me.milan.domain

import java.util.UUID

case class Key(value: String) extends AnyVal

object Key {
  def generate: Key = new Key(UUID.randomUUID().toString)
}
