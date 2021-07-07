package ru.delimobil.cabbit.client

import cats.effect.Blocker
import cats.effect.ContextShift
import cats.effect.Sync
import com.rabbitmq.client
import ru.delimobil.cabbit.algebra.ChannelOnPool

/* Channel instances must not be shared between threads */
final class RabbitClientChannelOnPool[F[_]: Sync: ContextShift] private[client] (
  channel: client.Channel,
  blocker: Blocker,
) extends ChannelOnPool[F] {

  def delay[V](f: client.Channel => V): F[V] =
    blocker.delay(f(channel))

  def blockOn[V](f: client.Channel => F[V]): F[V] =
    blocker.blockOn(f(channel))
}
