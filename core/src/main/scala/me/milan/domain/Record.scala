package me.milan.domain

case class Record[V](
  topic: Topic,
  key: Key,
  value: V,
  timestamp: Long,
  partitionId: Option[Integer] = None
)
