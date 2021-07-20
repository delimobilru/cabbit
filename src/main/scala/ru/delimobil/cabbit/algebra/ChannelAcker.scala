package ru.delimobil.cabbit.algebra

trait ChannelAcker[F[_]] extends ShutdownNotifier[F] {

  def basicAck(deliveryTag: DeliveryTag, multiple: Boolean): F[Unit]

  def basicNack(deliveryTag: DeliveryTag, multiple: Boolean, requeue: Boolean): F[Unit]

  def basicReject(deliveryTag: DeliveryTag, requeue: Boolean): F[Unit]

  def basicCancel(consumerTag: ConsumerTag): F[Unit]
}
