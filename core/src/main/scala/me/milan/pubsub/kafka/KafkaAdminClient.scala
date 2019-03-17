package me.milan.pubsub.kafka

import java.util.Properties

import scala.collection.JavaConverters._

import cats.effect.ConcurrentEffect
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import org.apache.kafka.clients.admin.AdminClientConfig._
import org.apache.kafka.clients.admin.{ AdminClient, NewTopic }
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.{ Done, Error, Topic }

class KafkaAdminClient[F[_]](
  config: KafkaConfig
)(
  implicit
  E: ConcurrentEffect[F]
) {

  private val props = new Properties()
  props.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))

  private val adminClient: AdminClient = AdminClient.create(props)

  def createTopics: F[Done] = createTopics(config.topics.toSet)

  def createTopics(topicConfigs: Set[TopicConfig]): F[Done] = {

    val topicDefaultConfig: Map[String, String] = Map(
      "delete.retention.ms" → Long.MaxValue.toString,
      "retention.ms" → Long.MaxValue.toString
    )

    val newTopics = topicConfigs.map { topicConfig ⇒
      new NewTopic(
        topicConfig.name.value,
        topicConfig.partitions.value,
        topicConfig.replicationFactor.value.toShort
      ).configs(topicDefaultConfig.asJava)
    }

    E.async[Done] { cb ⇒
        adminClient
          .createTopics(newTopics.asJavaCollection)
          .all()
          .whenComplete { (_, throwable) ⇒
            cb(Option(throwable).toLeft(Done.instance))
          }
        ()
      }
      .handleError {
        case _: org.apache.kafka.common.errors.TopicExistsException ⇒ Done
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }
  }

  def createTopic(topicConfig: TopicConfig): F[Done] = createTopics(Set(topicConfig))

  def getTopics(ignoreSystemTopics: Boolean = true): F[Set[Topic]] =
    E.async[Set[Topic]] { cb ⇒
        adminClient.listTopics().names().whenComplete { (topics, throwable) ⇒
          cb(
            Option(throwable)
              .toLeft(
                topics.asScala.toSet
                  .filterNot(_.startsWith("_") && ignoreSystemTopics)
                  .map(Topic)
              )
          )
        }
        ()
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }

  def deleteTopic(topic: Topic): F[Done] = deleteTopics(Set(topic))

  def deleteAllTopics: F[Done] =
    for {
      topics ← getTopics()
      _ ← deleteTopics(topics)
    } yield Done

  private def deleteTopics(topics: Set[Topic]): F[Done] =
    E.async[Done] { cb ⇒
        adminClient
          .deleteTopics(topics.map(_.value).asJavaCollection)
          .all
          .whenComplete { (_, throwable) ⇒
            cb(Option(throwable).toLeft(Done))
          }
        ()
      }
      .recover {
        case _: UnknownTopicOrPartitionException ⇒ Done
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }

}
