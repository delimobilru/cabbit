package ru.delimobil.cabbit.util

import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Sync
import cats.implicits.toFlatMapOps
import cats.implicits.toFunctorOps
import ru.delimobil.cabbit.ConnectionFactoryProvider
import ru.delimobil.cabbit.algebra.ChannelConsumer
import ru.delimobil.cabbit.algebra.ChannelDeclaration
import ru.delimobil.cabbit.algebra.ChannelPublisher
import ru.delimobil.cabbit.config.declaration.BindDeclaration
import ru.delimobil.cabbit.config.declaration.ExchangeDeclaration
import ru.delimobil.cabbit.config.declaration.QueueDeclaration
import ru.delimobil.cabbit.util.Config.AmqpConfig

object Helpers {

  def makeConsumer[F[_]: ContextShift: ConcurrentEffect](
    amqpConfig: AmqpConfig,
  ): Resource[F, ChannelConsumer[F]] = {
    val rabbitFactory = ConnectionFactoryProvider.provide[F](amqpConfig.toCabbitConfig)
    for {
      connection <- rabbitFactory.newConnection
      _ <- Resource.eval(
        connection.createChannelDeclaration.use { channel =>
          for {
            declarations <- declare(amqpConfig, channel)
            (exchange, queue, bind) = declarations
            exchangeResult <- channel.exchangeDeclare(exchange)
            queueResult <- channel.queueDeclare(queue)
            bindResult <- channel.queueBind(bind)
          } yield (exchangeResult, queueResult, bindResult)
        },
      )
      consumer <- connection.createChannelConsumer
    } yield consumer
  }

  def makePublisher[F[_]: ContextShift: ConcurrentEffect](
    amqpConfig: AmqpConfig,
  ): Resource[F, ChannelPublisher[F]] = {
    val rabbitFactory = ConnectionFactoryProvider.provide[F](amqpConfig.toCabbitConfig)
    for {
      connection <- rabbitFactory.newConnection
      _ <- Resource.eval(
        connection.createChannelDeclaration.use { channel =>
          for {
            declarations <- declare(amqpConfig, channel)
            (exchange, queue, bind) = declarations
            exchangeResult <- channel.exchangeDeclare(exchange)
            queueResult <- channel.queueDeclare(queue)
            bindResult <- channel.queueBind(bind)
          } yield (exchangeResult, queueResult, bindResult)
        },
      )
      publisher <- connection.createChannelPublisher
    } yield publisher
  }

  private def declare[F[_]](amqpConfig: AmqpConfig, declaration: ChannelDeclaration[F])(implicit
    F: Sync[F],
  ): F[(ExchangeDeclaration, QueueDeclaration, BindDeclaration)] =
    for {
      declarations <- F.delay(
        (
          amqpConfig.toExchangeDeclaration,
          amqpConfig.toQueueDeclaration,
          amqpConfig.toBindDeclaration,
        ),
      )
      (exchange, queue, binding) = declarations
      _ <- declaration.exchangeDeclare(exchange)
      _ <- declaration.queueDeclare(queue)
      _ <- declaration.queueBind(binding)
    } yield (exchange, queue, binding)
}
