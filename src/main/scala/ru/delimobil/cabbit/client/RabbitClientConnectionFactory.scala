package ru.delimobil.cabbit.client

import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Sync
import com.rabbitmq.client
import ru.delimobil.cabbit.algebra.Connection
import ru.delimobil.cabbit.algebra.ConnectionFactory
import ru.delimobil.cabbit.config.CabbitConfig

import scala.jdk.CollectionConverters._

final class RabbitClientConnectionFactory[F[_]: ConcurrentEffect: ContextShift](
  config: CabbitConfig,
) extends ConnectionFactory[F] {

  private val factory = new client.ConnectionFactory()

  private val firstNode = config.nodes.head
  factory.setHost(firstNode.host)
  factory.setPort(firstNode.port)
  factory.setVirtualHost(config.virtualHost)
  factory.setConnectionTimeout(config.connectionTimeout.toMillis.toInt)
  factory.setAutomaticRecoveryEnabled(config.automaticRecovery)
  if (config.sslConfig.ssl) config.sslConfig.context.fold(factory.useSslProtocol())(factory.useSslProtocol)
  factory.setSaslConfig(config.sslConfig.saslConfig)
  config.username.foreach(factory.setUsername)
  config.password.foreach(factory.setPassword)

  private val addresses = config.nodes.map(node => new client.Address(node.host, node.port)).toList

  def newConnection: Resource[F, Connection[F]] =
    Resource
      .make(Sync[F].delay(factory.newConnection(addresses.asJava)))(c => Sync[F].delay(c.close()))
      .map(new RabbitClientConnection[F](_))
}
