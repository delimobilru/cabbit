package ru.delimobil.cabbit.algebra

import com.rabbitmq.client
import ru.delimobil.cabbit.config.declaration.BindDeclaration
import ru.delimobil.cabbit.config.declaration.ExchangeDeclaration
import ru.delimobil.cabbit.config.declaration.QueueDeclaration

trait ChannelDeclaration[F[_]] extends ShutdownNotifier[F] {

  def queueDeclare(queueDeclaration: QueueDeclaration): F[client.AMQP.Queue.DeclareOk]

  def exchangeDeclare(exchangeDeclaration: ExchangeDeclaration): F[Unit]

  def queueBind(queueBind: BindDeclaration): F[Unit]

  def queueUnbind(bind: BindDeclaration): F[Unit]

  def queueDelete(queueName: QueueName): F[client.AMQP.Queue.DeleteOk]

  def exchangeDelete(exchangeName: ExchangeName): F[Unit]
}
