package ru.delimobil.cabbit.client

import com.rabbitmq.client
import ru.delimobil.cabbit.algebra.BodyEncoder
import ru.delimobil.cabbit.algebra.ChannelOnPool
import ru.delimobil.cabbit.algebra.ChannelPublisher
import ru.delimobil.cabbit.algebra.ExchangeName
import ru.delimobil.cabbit.algebra.RoutingKey

final class RabbitClientChannelPublisher[F[_]](
  channelOnPool: ChannelOnPool[F],
) extends ChannelPublisher[F] {

  def basicPublish[V](
    exchangeName: ExchangeName,
    routingKey: RoutingKey,
    properties: client.AMQP.BasicProperties,
    body: V,
  )(implicit encoder: BodyEncoder[V]): F[Unit] =
    basicPublish(exchangeName, routingKey, properties, mandatory = false, body)

  def basicPublish[V](
    exchangeName: ExchangeName,
    routingKey: RoutingKey,
    properties: client.AMQP.BasicProperties,
    mandatory: Boolean,
    body: V,
  )(implicit encoder: BodyEncoder[V]): F[Unit] = {
    val props =
      properties
        .builder()
        .contentType(encoder.contentType.raw)
        .contentEncoding(encoder.contentType.raw)
        .build()

    channelOnPool.delay(_.basicPublish(exchangeName.name, routingKey.name, mandatory, props, encoder.encode(body)))
  }
}
