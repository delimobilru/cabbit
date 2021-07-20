package ru.delimobil.cabbit.client

import cats.effect.ConcurrentEffect
import cats.effect.syntax.effect._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._
import com.rabbitmq.client
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException
import fs2.Stream
import fs2.concurrent.NoneTerminatedQueue
import fs2.concurrent.Queue
import ru.delimobil.cabbit.algebra.ChannelConsumer
import ru.delimobil.cabbit.algebra.ChannelOnPool
import ru.delimobil.cabbit.algebra.ConsumerTag
import ru.delimobil.cabbit.algebra.DeliveryTag
import ru.delimobil.cabbit.algebra.QueueName

final class RabbitClientChannelConsumer[F[_]: ConcurrentEffect](
  channelOnPool: ChannelOnPool[F]
) extends ChannelConsumer[F] {

  def basicQos(prefetchCount: Int): F[Unit] =
    channelOnPool.delay(_.basicQos(prefetchCount))

  def basicConsume(
    queue: QueueName,
    deliverCallback: client.DeliverCallback,
    cancelCallback: client.CancelCallback
  ): F[ConsumerTag] =
    channelOnPool.delay(_.basicConsume(queue.name, deliverCallback, cancelCallback)).map(ConsumerTag)

  def basicConsume(queue: QueueName, consumer: client.Consumer): F[ConsumerTag] =
    channelOnPool.delay(_.basicConsume(queue.name, consumer)).map(ConsumerTag)

  def basicGet(queue: QueueName, autoAck: Boolean): F[client.GetResponse] =
    channelOnPool.delay(_.basicGet(queue.name, autoAck))

  def deliveryStream(
    queueName: QueueName,
    prefetchCount: Int
  ): F[(ConsumerTag, Stream[F, client.Delivery])] =
    for {
      _ <- basicQos(prefetchCount)
      queue <- Queue.boundedNoneTerminated[F, client.Delivery](prefetchCount)
      consumer = getConsumer(queue)
      tag <- basicConsume(queueName, consumer)
    } yield (tag, queue.dequeue)

  def basicAck(deliveryTag: DeliveryTag, multiple: Boolean): F[Unit] =
    channelOnPool.delay(_.basicAck(deliveryTag.number, multiple))

  def basicNack(deliveryTag: DeliveryTag, multiple: Boolean, requeue: Boolean): F[Unit] =
    channelOnPool.delay(_.basicNack(deliveryTag.number, multiple, requeue))

  def basicReject(deliveryTag: DeliveryTag, requeue: Boolean): F[Unit] =
    channelOnPool.delay(_.basicReject(deliveryTag.number, requeue))

  def basicCancel(consumerTag: ConsumerTag): F[Unit] =
    channelOnPool.delay(_.basicCancel(consumerTag.name))

  def isOpen: F[Boolean] =
    channelOnPool.delay(_.isOpen)

  private def getCallbacks(
    queue: NoneTerminatedQueue[F, client.Delivery]
  ): (client.CancelCallback, client.DeliverCallback) = {
    val cancelCallback: client.CancelCallback = _ => queue.enqueue1(none).toIO.unsafeRunSync()
    val deliverCallback: client.DeliverCallback = (_, delivery) => queue.enqueue1(delivery.some).toIO.unsafeRunSync()
    (cancelCallback, deliverCallback)
  }

  private def getConsumer(queue: NoneTerminatedQueue[F, client.Delivery]): Consumer = {
    val (cancel, deliver) = getCallbacks(queue)

    new client.Consumer {

      def handleConsumeOk(consumerTag: String): Unit = {}

      def handleCancelOk(consumerTag: String): Unit =
        cancel.handle(consumerTag)

      def handleCancel(consumerTag: String): Unit =
        cancel.handle(consumerTag)

      def handleShutdownSignal(consumerTag: String, sig: ShutdownSignalException): Unit =
        cancel.handle(consumerTag)

      def handleRecoverOk(consumerTag: String): Unit = {}

      def handleDelivery(
        consumerTag: String,
        envelope: Envelope,
        properties: AMQP.BasicProperties,
        body: Array[Byte]
      ): Unit = deliver.handle(consumerTag, new client.Delivery(envelope, properties, body))
    }
  }
}
