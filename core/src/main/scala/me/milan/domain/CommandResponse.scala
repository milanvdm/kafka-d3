package me.milan.domain

case class CommandResponse[R](
  id: Key,
  response: R
)
