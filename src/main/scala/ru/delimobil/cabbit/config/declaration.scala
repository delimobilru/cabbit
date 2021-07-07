package ru.delimobil.cabbit.config

import com.rabbitmq.client.BuiltinExchangeType
import ru.delimobil.cabbit.algebra.ExchangeName
import ru.delimobil.cabbit.algebra.QueueName
import ru.delimobil.cabbit.algebra.RoutingKey

object declaration {

  sealed abstract class DurableConfig(val bool: Boolean)

  object DurableConfig {
    object Durable extends DurableConfig(true)
    object NonDurable extends DurableConfig(false)
  }

  sealed abstract class ExclusiveConfig(val bool: Boolean)

  object ExclusiveConfig {
    object Exclusive extends ExclusiveConfig(true)
    object NonExclusive extends ExclusiveConfig(false)
  }

  sealed abstract class AutoDeleteConfig(val bool: Boolean)

  object AutoDeleteConfig {
    object AutoDelete extends AutoDeleteConfig(true)
    object NonAutoDelete extends AutoDeleteConfig(false)
  }

  sealed abstract class InternalConfig(val bool: Boolean)

  object InternalConfig {
    object Internal extends InternalConfig(true)
    object NonInternal extends InternalConfig(false)
  }

  type Arguments = Map[String, Any]

  case class QueueDeclaration(
    queueName: QueueName,
    durable: DurableConfig,
    exclusive: ExclusiveConfig,
    autoDelete: AutoDeleteConfig,
    arguments: Arguments,
  )

  case class ExchangeDeclaration(
    exchangeName: ExchangeName,
    exchangeType: BuiltinExchangeType,
    durable: DurableConfig,
    autoDelete: AutoDeleteConfig,
    internal: InternalConfig,
    arguments: Arguments,
  )

  case class BindDeclaration(
    queueName: QueueName,
    exchangeName: ExchangeName,
    routingKey: RoutingKey,
  )
}
