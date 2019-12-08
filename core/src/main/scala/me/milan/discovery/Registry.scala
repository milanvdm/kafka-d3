package me.milan.discovery

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}

import cats.effect.{Sync, Timer}
import com.orbitz.consul.Consul
import com.orbitz.consul.model.agent.{ImmutableRegistration, Registration}
import fs2.Stream
import org.http4s.Uri

import me.milan.domain.Key

object Registry {

  case class RegistryGroup(value: String) extends AnyVal

  //TODO: Create ConsulRegistry

}

trait Registry[F[_]] {
  import Registry._

  def register(serviceGroup: RegistryGroup): Stream[F, Unit]

  def getHosts(serviceGroup: RegistryGroup): F[List[Uri]]

}

private[discovery] class ConsulRegistry[F[_]: Sync: Timer](
  serviceId: Key,
  timeToLive: FiniteDuration,
  consulClient: Consul
) extends Registry[F] {
  import Registry._

  private val agentClient = consulClient.agentClient
  private val healthClient = consulClient.healthClient

  override def register(serviceGroup: RegistryGroup): Stream[F, Unit] = {
    val service = ImmutableRegistration.builder
      .id(serviceId.value)
      .name(serviceGroup.value)
      .port(8080) //TODO get port from config
      .check(Registration.RegCheck.ttl(timeToLive.toSeconds))
      .build()

    for {
      _ <- Stream.eval(Sync[F].delay(agentClient.register(service)))
      repeat <- (checkIn ++ Stream.sleep_(timeToLive / 2)).repeat
    } yield repeat
  }

  // TODO: Use callback to make Async
  override def getHosts(serviceGroup: RegistryGroup): F[List[Uri]] = Sync[F].delay(
    healthClient
      .getHealthyServiceInstances(serviceGroup.value)
      .getResponse.asScala.toList
      .map(_.getNode.getAddress)
      .map(Uri.unsafeFromString)
  )

  private def checkIn: Stream[F, Unit] = Stream.retry(
    Sync[F].delay(agentClient.pass(serviceId.value)),
    100.millis,
    x => x,
    maxAttempts = 5
  )
}
