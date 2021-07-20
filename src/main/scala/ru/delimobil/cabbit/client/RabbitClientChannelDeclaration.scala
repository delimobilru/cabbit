package ru.delimobil.cabbit.client

import cats.Functor
import cats.syntax.functor._
import com.rabbitmq.client
import ru.delimobil.cabbit.algebra.ChannelDeclaration
import ru.delimobil.cabbit.algebra.ChannelOnPool
import ru.delimobil.cabbit.algebra.ExchangeName
import ru.delimobil.cabbit.algebra.QueueName
import ru.delimobil.cabbit.config.declaration.BindDeclaration
import ru.delimobil.cabbit.config.declaration.ExchangeDeclaration
import ru.delimobil.cabbit.config.declaration.QueueDeclaration

import scala.jdk.CollectionConverters._

final class RabbitClientChannelDeclaration[F[_]: Functor](
  channel: ChannelOnPool[F]
) extends ChannelDeclaration[F] {

  def queueDeclare(queueDeclaration: QueueDeclaration): F[client.AMQP.Queue.DeclareOk] =
    channel.delay {
      _.queueDeclare(
        queueDeclaration.queueName.name,
        queueDeclaration.durable.bool,
        queueDeclaration.exclusive.bool,
        queueDeclaration.autoDelete.bool,
        queueDeclaration.arguments.asJava
      )
    }

  def exchangeDeclare(exchangeDeclaration: ExchangeDeclaration): F[Unit] =
    channel.delay {
      _.exchangeDeclare(
        exchangeDeclaration.exchangeName.name,
        exchangeDeclaration.exchangeType,
        exchangeDeclaration.durable.bool,
        exchangeDeclaration.autoDelete.bool,
        exchangeDeclaration.internal.bool,
        exchangeDeclaration.arguments.asJava
      )
    }.void

  def queueBind(bindDeclaration: BindDeclaration): F[Unit] =
    channel.delay {
      _.queueBind(
        bindDeclaration.queueName.name,
        bindDeclaration.exchangeName.name,
        bindDeclaration.routingKey.name
      )
    }.void

  def queueUnbind(bind: BindDeclaration): F[Unit] =
    channel.delay(_.queueUnbind(bind.queueName.name, bind.exchangeName.name, bind.routingKey.name))

  def queueDelete(queueName: QueueName): F[client.AMQP.Queue.DeleteOk] =
    channel.delay(_.queueDelete(queueName.name))

  def exchangeDelete(exchangeName: ExchangeName): F[Unit] =
    channel.delay(_.exchangeDelete(exchangeName.name)).void

  def isOpen: F[Boolean] =
    channel.delay(_.isOpen)
}
