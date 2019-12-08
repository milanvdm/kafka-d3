package me.milan.clients.kafka

import java.time.OffsetDateTime
import java.util.Properties

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

import cats.effect.{ ConcurrentEffect, Sync }
import cats.instances.long._
import cats.syntax.applicativeError._
import cats.syntax.monadError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.AdminClientConfig._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ KafkaConsumer, OffsetAndMetadata }
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.{ ConsumerGroupState, TopicPartition }
import org.http4s.Uri
import cats.syntax.traverse._
import cats.instances.list._

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.Topic
import me.milan.pubsub.kafka.KConsumer
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId
import me.milan.domain.Error

object KafkaAdminClient {

  sealed trait LogOffsetResult
  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  def apply[F[_]: ConcurrentEffect](config: KafkaConfig): F[KafkaAdminClient[F]] =
    for {
      properties <- Sync[F].delay {
        val props: Properties = new Properties()
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.show).mkString(","))
        props
      }
      adminClient <- Sync[F].delay(AdminClient.create(properties))
    } yield new KafkaAdminClient[F](config, adminClient)

}

class KafkaAdminClient[F[_]: ConcurrentEffect](
  config: KafkaConfig,
  adminClient: AdminClient
) {
  import KafkaAdminClient._

  def createTopics: F[Unit] = createTopics(config.topics.toSet)

  def createTopics(topicConfigs: Set[TopicConfig]): F[Unit] = {

    val newTopics = topicConfigs.map { topicConfig =>
      new NewTopic(
        topicConfig.name.value,
        topicConfig.partitions.value,
        topicConfig.replicationFactor.value.toShort
      ).configs(
        Map(
          "delete.retention.ms" -> topicConfig.retention.toMillis.show,
          "retention.ms" -> topicConfig.retention.toMillis.show,
          "message.timestamp.type" -> "CreateTime"
        ).asJava
      )
    }

    adminClient
      .createTopics(newTopics.asJavaCollection)
      .values()
      .asScala
      .values
      .toList
      .map { future =>
        ConcurrentEffect[F]
          .async[Unit] { cb =>
            future
              .whenComplete { (_, throwable) =>
                cb(Option(throwable).toLeft(()))
              }
            ()
          }
          .handleError {
            case _: org.apache.kafka.common.errors.TopicExistsException => ()
          }
      }
      .sequence
      .void

  }

  def createTopic(topicConfig: TopicConfig): F[Unit] = createTopics(Set(topicConfig))

  def getTopics(ignoreSystemTopics: Boolean = true): F[Set[Topic]] =
    ConcurrentEffect[F].async[Set[Topic]] { cb =>
      adminClient.listTopics().names().whenComplete { (topics, throwable) =>
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

  def deleteTopic(topic: Topic): F[Unit] = deleteTopics(Set(topic))

  def deleteAllTopics: F[Unit] =
    for {
      topics <- getTopics()
      _ <- deleteTopics(topics)
    } yield ()

  private def deleteTopics(topics: Set[Topic]): F[Unit] =
    adminClient
      .deleteTopics(topics.map(_.value).asJavaCollection)
      .values()
      .asScala
      .values
      .toList
      .map { future =>
        ConcurrentEffect[F]
          .async[Unit] { cb =>
            future
              .whenComplete { (_, throwable) =>
                cb(Option(throwable).toLeft(()))
              }
            ()
          }
          .recover {
            case _: UnknownTopicOrPartitionException => ()
          }
      }
      .sequence
      .void

  def consumerGroupHosts(consumerGroupId: ConsumerGroupId): F[Set[Uri]] =
    consumerGroupMembers(consumerGroupId)
      .map(_.map(_.host).map(Uri.unsafeFromString))

  def consumerGroupMembers(consumerGroupId: ConsumerGroupId): F[Set[MemberDescription]] =
    ConcurrentEffect[F]
      .async[Option[ConsumerGroupDescription]] { cb =>
        adminClient
          .describeConsumerGroups(Seq(consumerGroupId.value).asJavaCollection)
          .all
          .whenComplete { (consumerGroups, throwable) =>
            cb(Option(throwable).toLeft(consumerGroups.asScala.values.headOption))
          }
        ()
      }
      .map(_.map(_.members.asScala.toSet))
      .map(_.getOrElse(Set.empty))

  // TODO: Should be fixed in Kafka 2.5 (exposed method in AdminClient)
  // Based on https://github.com/apache/kafka/blob/2.1.1/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala#L300
  def resetConsumerGroup(
    consumerGroupId: ConsumerGroupId,
    topic: Topic,
    timestamp: OffsetDateTime
  ): F[Unit] =
    for {
      consumer <- KConsumer(config, consumerGroupId).map(_.consumer)
      consumerGroup <- getConsumerGroup(consumerGroupId)
      _ <- isRunning(consumerGroup)
      partitionsToReset <- partitions(topic, consumer)
      partitionsWithTimestampReset = resetPartitionsToTimestamp(consumer, partitionsToReset, timestamp)
    } yield {
      consumer.commitSync(partitionsWithTimestampReset.asJava)
    }

  private def getConsumerGroup(consumerGroupId: ConsumerGroupId): F[ConsumerGroupDescription] =
    ConcurrentEffect[F]
      .async[Option[ConsumerGroupDescription]] { cb =>
        adminClient
          .describeConsumerGroups(
            List(consumerGroupId.value).asJavaCollection
          )
          .all
          .whenComplete { (consumerGroups, throwable) =>
            cb(Option(throwable).toLeft(consumerGroups.asScala.get(consumerGroupId.value)))
          }
        ()
      }
      .flatMap {
        case Some(consumerGroup) => ConcurrentEffect[F].pure(consumerGroup)
        case None                => ConcurrentEffect[F].raiseError(new IllegalArgumentException("Cannot find the consumerGroupId"))
      }

  private def isRunning(consumerGroup: ConsumerGroupDescription): F[Unit] =
    consumerGroup.state match {
      case ConsumerGroupState.DEAD | ConsumerGroupState.EMPTY => ConcurrentEffect[F].pure(())
      case _                                                  => ConcurrentEffect[F].raiseError(new IllegalStateException("Cannot reset a running consumer-group"))
    }

  private def partitions(
    topic: Topic,
    consumer: KafkaConsumer[String, GenericRecord]
  ): F[List[TopicPartition]] =
    ConcurrentEffect[F]
      .delay(
        consumer
          .partitionsFor(topic.value, 1.second.toJava)
          .asScala
          .map(partitionInfo => new TopicPartition(topic.value, partitionInfo.partition))
          .toList
      )
      .adaptError {
        case _: java.lang.NullPointerException => Error.TopicNotFound
      }

  private def resetPartitionsToTimestamp(
    consumer: KafkaConsumer[String, GenericRecord],
    partitions: List[TopicPartition],
    timestamp: OffsetDateTime
  ): Map[TopicPartition, OffsetAndMetadata] = {
    consumer.assign(partitions.asJava)

    val timestampEpoch = Long.box(timestamp.toInstant.toEpochMilli)

    val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
      consumer
        .offsetsForTimes(partitions.map(_ -> timestampEpoch).toMap.asJava)
        .asScala
        .partition(_._2 != null)

    val successfulLogTimestampOffsets = successfulOffsetsForTimes
      .mapValues(offsetAndTimestamp => LogOffsetResult.LogOffset(offsetAndTimestamp.offset))
      .toMap

    val endOffsets = consumer
      .endOffsets(unsuccessfulOffsetsForTimes.keySet.asJava)
      .asScala
      .toMap

    val unsuccessfulLogTimestampOffsets = unsuccessfulOffsetsForTimes.keySet.map { topicPartition =>
      endOffsets.get(topicPartition) match {
        case Some(logEndOffset) => topicPartition -> LogOffsetResult.LogOffset(logEndOffset)
        case None               => topicPartition -> LogOffsetResult.Unknown
      }
    }.toMap

    val logTimestampOffsets = successfulLogTimestampOffsets ++ unsuccessfulLogTimestampOffsets

    partitions.flatMap { topicPartition =>
      val logTimestampOffset = logTimestampOffsets.get(topicPartition)
      logTimestampOffset match {
        case Some(LogOffsetResult.LogOffset(offset)) => List(topicPartition -> new OffsetAndMetadata(offset))
        case _                                       => List.empty
      }
    }.toMap

  }

}
