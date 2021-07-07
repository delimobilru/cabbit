package ru.delimobil.cabbit.util

import cats.data.NonEmptyList
import com.rabbitmq.client.BuiltinExchangeType
import ru.delimobil.cabbit.algebra.ExchangeName
import ru.delimobil.cabbit.algebra.QueueName
import ru.delimobil.cabbit.algebra.RoutingKey
import ru.delimobil.cabbit.config.CabbitConfig
import ru.delimobil.cabbit.config.CabbitConfig.CabbitNodeConfig
import ru.delimobil.cabbit.config.CabbitConfig.SslConfig
import ru.delimobil.cabbit.config.declaration.AutoDeleteConfig
import ru.delimobil.cabbit.config.declaration.BindDeclaration
import ru.delimobil.cabbit.config.declaration.DurableConfig
import ru.delimobil.cabbit.config.declaration.ExchangeDeclaration
import ru.delimobil.cabbit.config.declaration.ExclusiveConfig
import ru.delimobil.cabbit.config.declaration.InternalConfig
import ru.delimobil.cabbit.config.declaration.QueueDeclaration

import scala.concurrent.duration.FiniteDuration

object Config {

  final case class ExchangeConfig(
    name: String,
    `type`: String,
    durable: Boolean,
    autoDelete: Boolean,
    internal: Boolean,
  )

  final case class QueueConfig(
    name: String,
    durable: Boolean,
    exclusive: Boolean,
    autoDelete: Boolean,
    prefetchCount: Int,
  )

  // same as CabbitConfig, but without SSL configuration
  final case class AmqpConfig(
    nodes: List[CabbitNodeConfig],
    virtualHost: String,
    connectionTimeout: FiniteDuration,
    username: Option[String],
    password: Option[String],
    automaticRecovery: Boolean = true,
    routingKey: String,
    exchange: ExchangeConfig,
    queue: QueueConfig,
  )

  implicit final class AmqpConfigOps(private val amqpConfig: AmqpConfig) extends AnyVal {

    def toCabbitConfig: CabbitConfig =
      CabbitConfig(
        nodes = NonEmptyList.fromListUnsafe(amqpConfig.nodes),
        virtualHost = amqpConfig.virtualHost,
        connectionTimeout = amqpConfig.connectionTimeout,
        username = amqpConfig.username,
        password = amqpConfig.password,
        sslConfig = SslConfig.default,
        automaticRecovery = amqpConfig.automaticRecovery,
      )

    def toBindDeclaration: BindDeclaration =
      BindDeclaration(
        queueName = QueueName(amqpConfig.queue.name),
        exchangeName = ExchangeName(amqpConfig.exchange.name),
        routingKey = RoutingKey(amqpConfig.routingKey),
      )

    def toExchangeDeclaration: ExchangeDeclaration =
      ExchangeDeclaration(
        exchangeName = ExchangeName(amqpConfig.exchange.name),
        exchangeType = BuiltinExchangeType.valueOf(amqpConfig.exchange.`type`.toUpperCase),
        durable = if (amqpConfig.exchange.durable) DurableConfig.Durable else DurableConfig.NonDurable,
        autoDelete =
          if (amqpConfig.exchange.autoDelete) AutoDeleteConfig.AutoDelete else AutoDeleteConfig.NonAutoDelete,
        internal = if (amqpConfig.exchange.internal) InternalConfig.Internal else InternalConfig.NonInternal,
        arguments = Map.empty,
      )

    def toQueueDeclaration: QueueDeclaration =
      QueueDeclaration(
        queueName = QueueName(amqpConfig.queue.name),
        durable = if (amqpConfig.exchange.durable) DurableConfig.Durable else DurableConfig.NonDurable,
        exclusive = if (amqpConfig.queue.exclusive) ExclusiveConfig.Exclusive else ExclusiveConfig.NonExclusive,
        autoDelete =
          if (amqpConfig.exchange.autoDelete) AutoDeleteConfig.AutoDelete else AutoDeleteConfig.NonAutoDelete,
        arguments = Map.empty,
      )
  }
}
