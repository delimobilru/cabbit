package ru.delimobil.cabbit

import cats.effect.IO
import cats.effect.Timer
import cats.instances.list._
import cats.syntax.apply._
import cats.syntax.traverse._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.BuiltinExchangeType
import ru.delimobil.cabbit.algebra.BodyEncoder.instances.jsonGzip
import ru.delimobil.cabbit.algebra.Connection
import ru.delimobil.cabbit.algebra.QueueName
import ru.delimobil.cabbit.algebra._
import ru.delimobil.cabbit.config.declaration._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object TestUtils {
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  def channelWithPublishedMessage(connection: Connection[IO])(messages: List[String])(
    assertF: (ChannelDeclaration[IO], ChannelConsumer[IO], QueueDeclaration) => IO[Unit],
  ): Unit = {
    val tuple =
      (connection.createChannelDeclaration, connection.createChannelConsumer, connection.createChannelPublisher)
    val action =
      tuple.tupled
        .use { case (declaration, consumerChannel, publisherChannel) =>
          for {
            declarations <- TestUtils.declare(declaration)
            (_, queue, bind) = declarations
            _ <- messages.traverse { message =>
              publisherChannel.basicPublish(
                bind.exchangeName,
                bind.routingKey,
                new BasicProperties,
                mandatory = true,
                message,
              )
            }
            _ <- IO.sleep(50.millis)
            _ <- assertF(declaration, consumerChannel, queue)
          } yield {}
        }

    action.unsafeRunSync()
  }

  def getDeclarations(): (ExchangeDeclaration, QueueDeclaration, BindDeclaration) =
    getDeclarations(UUID.randomUUID())

  def getDeclarations(uuid: UUID): (ExchangeDeclaration, QueueDeclaration, BindDeclaration) = {
    val exchange =
      ExchangeDeclaration(
        ExchangeName(s"the-exchange-$uuid"),
        BuiltinExchangeType.DIRECT,
        DurableConfig.NonDurable,
        AutoDeleteConfig.AutoDelete,
        InternalConfig.NonInternal,
        Map.empty,
      )

    val queue =
      QueueDeclaration(
        QueueName(s"the-queue-$uuid"),
        DurableConfig.NonDurable,
        ExclusiveConfig.NonExclusive,
        AutoDeleteConfig.AutoDelete,
        Map.empty,
      )

    val binding = BindDeclaration(queue.queueName, exchange.exchangeName, RoutingKey("the-key"))

    (exchange, queue, binding)
  }

  def declare(declaration: ChannelDeclaration[IO]): IO[(ExchangeDeclaration, QueueDeclaration, BindDeclaration)] =
    for {
      declarations <- IO.delay(getDeclarations())
      (exchange, queue, binding) = declarations
      _ <- declaration.exchangeDeclare(exchange)
      _ <- declaration.queueDeclare(queue)
      _ <- declaration.queueBind(binding)
    } yield (exchange, queue, binding)
}
