package me.milan.pubsub.kafka

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.either._
import pureconfig.generic.auto._

import me.milan.clients.kafka.KafkaAdminClient
import me.milan.config.ApplicationConfig
import me.milan.domain.Error

object KafkaBoot extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val program = for {
      config ← IO.fromEither(parseConfig)
      adminClient = new KafkaAdminClient[IO](config.kafka)
      result ← adminClient.createTopics
    } yield result

    program.map(_ ⇒ ExitCode.Success)
  }

  def parseConfig: Error Either ApplicationConfig =
    pureconfig.loadConfig[ApplicationConfig].leftMap(Error.IncorrectConfig)

}
