package me.milan.commands

import me.milan.domain.Record

trait Command {

  def verify[F[_], V]: F[Record[V]]

}
