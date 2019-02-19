package me.milan.writeside

// TODO: Contains a processor

object WriteSide {

  def aggregateById[A](
    processorName: String,
    key: String
  ): A = ???

  def stop(processorName: String) = ???

  //TODO: stopAll because processor is local

}

trait WriteSide {}

class KafkaWriteSide

class DummyWriteSide
