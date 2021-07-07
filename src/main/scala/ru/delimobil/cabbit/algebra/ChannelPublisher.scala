package ru.delimobil.cabbit.algebra

import com.rabbitmq.client

trait ChannelPublisher[F[_]] {

  def basicPublish[V](
    exchangeName: ExchangeName,
    routingKey: RoutingKey,
    properties: client.AMQP.BasicProperties,
    body: V,
  )(implicit encoder: BodyEncoder[V]): F[Unit]

  def basicPublish[V](
    exchangeName: ExchangeName,
    routingKey: RoutingKey,
    properties: client.AMQP.BasicProperties,
    mandatory: Boolean,
    body: V,
  )(implicit encoder: BodyEncoder[V]): F[Unit]
}
