package ru.delimobil.cabbit.client

import java.util.concurrent.Executors

import cats.effect.Blocker
import cats.effect.ConcurrentEffect
import cats.effect.ContextShift
import cats.effect.Resource
import cats.effect.Sync
import com.rabbitmq.client
import ru.delimobil.cabbit.algebra.ChannelConsumer
import ru.delimobil.cabbit.algebra.ChannelDeclaration
import ru.delimobil.cabbit.algebra.ChannelOnPool
import ru.delimobil.cabbit.algebra.ChannelPublisher
import ru.delimobil.cabbit.algebra.Connection

final class RabbitClientConnection[F[_]: ConcurrentEffect: ContextShift](
  raw: client.Connection,
) extends Connection[F] {

  private val getChannelExecutor =
    Sync[F].delay(
      Executors.newSingleThreadExecutor(runnable => {
        val thread = new Thread(runnable, s"rabbit-client-channel-${math.abs(hashCode)}")
        thread.setDaemon(true)
        thread
      }),
    )

  def createChannelDeclaration: Resource[F, ChannelDeclaration[F]] =
    createChannelOnPool.map(ch => new RabbitClientChannelDeclaration[F](ch))

  def createChannelPublisher: Resource[F, ChannelPublisher[F]] =
    createChannelOnPool.map(ch => new RabbitClientChannelPublisher[F](ch))

  def createChannelConsumer: Resource[F, ChannelConsumer[F]] =
    createChannelOnPool.map(ch => new RabbitClientChannelConsumer[F](ch))

  private def createChannelOnPool: Resource[F, ChannelOnPool[F]] =
    for {
      blocker <- Blocker.fromExecutorService(getChannelExecutor)
      rawChannel <- Resource.fromAutoCloseableBlocking(blocker)(Sync[F].delay(raw.createChannel()))
    } yield new RabbitClientChannelOnPool(rawChannel, blocker)
}
