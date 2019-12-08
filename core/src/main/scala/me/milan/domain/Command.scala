package me.milan.domain

abstract class Command[C](val id: Key) {

  def verify[F[_], E]: F[Record[E]]

}
