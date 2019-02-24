package me.milan.domain

import pureconfig.error.ConfigReaderFailures

sealed trait Error extends Exception

object Error {
  final case object HostNotFound extends Error
  final case class IncorrectConfig(underlying: ConfigReaderFailures) extends Error
  final case object KeyNotFound extends Error
  final case class TopicExists(underlying: Throwable) extends Error
  final case class System(underlying: Throwable) extends Error
}
