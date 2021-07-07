package ru.delimobil.cabbit.algebra

final case class QueueName(name: String) extends AnyVal

final case class ExchangeName(name: String) extends AnyVal

final case class RoutingKey(name: String) extends AnyVal

final case class ConsumerTag(name: String) extends AnyVal

final case class DeliveryTag(number: Long) extends AnyVal
