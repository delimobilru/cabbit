package ru.delimobil.cabbit

import cats.effect.Resource
import cats.effect.Sync
import com.dimafeng.testcontainers.RabbitMQContainer
import ru.delimobil.cabbit.config.CabbitConfig.Host
import ru.delimobil.cabbit.config.CabbitConfig.Port
import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import ru.delimobil.cabbit.algebra.Connection
import cats.data.NonEmptyList
import ru.delimobil.cabbit.config.CabbitConfig

import scala.concurrent.duration._

class RabbitContainer private {

  private val container: RabbitMQContainer = RabbitMQContainer()

  container.container.start()

  val host: Host = container.host

  val port: Port = container.amqpPort
}

object RabbitContainer {

  def make[F[_]: Sync]: Resource[F, RabbitContainer] =
    Resource.make(Sync[F].delay(new RabbitContainer))(provider => Sync[F].delay(provider.container.stop()))

  def makeConnection[F[_]: ConcurrentEffect: ContextShift](
    container: RabbitContainer
  ): Resource[F, Connection[F]] = {
    val nodes = NonEmptyList.one(CabbitConfig.CabbitNodeConfig(container.host, container.port))
    val config = CabbitConfig(nodes, virtualHost = "/", 60.seconds, username = None, password = None)
    val connectionFactory = ConnectionFactoryProvider.provide[F](config)
    connectionFactory.newConnection
  }
}
