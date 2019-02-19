package me.milan.domain

trait Aggregator[A, E] {

  def process(
    previousAggregate: Option[A],
    event: E
  ): A

}
