package ru.delimobil.cabbit

import cats.data.NonEmptyList
import cats.effect.ContextShift
import cats.effect.IO
import cats.effect.Resource
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import ru.delimobil.cabbit.algebra.ContentEncoding._
import ru.delimobil.cabbit.algebra.Connection
import ru.delimobil.cabbit.algebra.DeliveryTag
import ru.delimobil.cabbit.config.CabbitConfig
import ru.delimobil.cabbit.config.CabbitConfig.CabbitNodeConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class Fs2RabbitSuite extends AnyFunSuite with BeforeAndAfterAll {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val connectionResource: Resource[IO, Connection[IO]] =
    for {
      container <- RabbitContainerProvider.resource[IO]
      config = CabbitConfig(
        NonEmptyList.one(CabbitNodeConfig(container.host, container.port)),
        virtualHost = "/",
        connectionTimeout = 10.seconds,
        username = None,
        password = None,
      )
      connectionFactory = ConnectionFactoryProvider.provide[IO](config)
      newConnection <- connectionFactory.newConnection
    } yield newConnection

  val (connection, closeIO) = connectionResource.allocated.unsafeRunSync()

  test("queue declaration returns state") {
    val action =
      connection.createChannelDeclaration.use { channel =>
        for {
          declarations <- TestUtils.declare(channel)
          (_, queue, _) = declarations
          declareResult <- channel.queueDeclare(queue)
          _ = assert(declareResult.getQueue == queue.queueName.name)
          _ = assert(declareResult.getConsumerCount == 0)
          _ = assert(declareResult.getMessageCount == 0)
        } yield {}
      }

    action.unsafeRunSync()
  }

  test("publisher publishes") {
    TestUtils.channelWithPublishedMessage(connection)(List("hello from fs2-rabbit")) { case (declaration, _, queue) =>
      declaration.queueDeclare(queue).map(declareResult => assert(declareResult.getMessageCount == 1)) *> IO.unit
    }
  }

  test("consumer gets") {
    val message = "hello from fs2-rabbit"
    TestUtils.channelWithPublishedMessage(connection)(List(message)) { case (declaration, consumer, queue) =>
      for {
        response <- consumer.basicGet(queue.queueName, autoAck = true)
        declareOk <- declaration.queueDeclare(queue)
        bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
        _ = assert(bodyResponse === Right(Json.fromString(message)))
        _ = assert(declareOk.getConsumerCount === 0)
        _ = assert(declareOk.getMessageCount === 0)
      } yield {}
    }
  }

  test("consumer rejects get with requeue") {
    val message = "hello from fs2-rabbit"

    TestUtils.channelWithPublishedMessage(connection)(List(message)) { case (declaration, consumer, queue) =>
      for {
        response <- consumer.basicGet(queue.queueName, autoAck = false)
        _ <- consumer.basicReject(DeliveryTag(response.getEnvelope.getDeliveryTag), requeue = true)
        declareOk <- declaration.queueDeclare(queue)
        bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
        _ = assert(bodyResponse === Right(Json.fromString(message)))
        _ = assert(declareOk.getConsumerCount === 0)
        _ = assert(declareOk.getMessageCount === 1)
      } yield {}
    }
  }

  test("consumer rejects get without requeue") {
    val message = "hello from fs2-rabbit"

    TestUtils.channelWithPublishedMessage(connection)(List(message)) { case (declaration, consumer, queue) =>
      for {
        response <- consumer.basicGet(queue.queueName, autoAck = false)
        _ <- consumer.basicReject(DeliveryTag(response.getEnvelope.getDeliveryTag), requeue = false)
        declareOk <- declaration.queueDeclare(queue)
        bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
        _ = assert(bodyResponse === Right(Json.fromString(message)))
        _ = assert(declareOk.getConsumerCount === 0)
        _ = assert(declareOk.getMessageCount === 0)
      } yield {}
    }
  }

  test("consumer consumes") {
    val MaxMessages = 1000
    val Prefetched = 51
    val messages = (1 to MaxMessages).map(_ => Random.nextString(Random.nextInt(100))).toList

    TestUtils.channelWithPublishedMessage(connection)(messages) { case (declaration, consumer, queue) =>
      for {
        deliveryStream <- consumer.deliveryStream(queue.queueName, prefetchCount = Prefetched)
        (_, stream) = deliveryStream
        declareOk <- declaration.queueDeclare(queue)
        _ = assert(declareOk.getConsumerCount == 1)
        _ = assert(declareOk.getMessageCount == (MaxMessages - Prefetched))
        streamedMessages <-
          stream
            .evalTap(delivery => consumer.basicAck(DeliveryTag(delivery.getEnvelope.getDeliveryTag), multiple = false))
            .take(MaxMessages.toLong)
            .compile
            .toList
        streamedBodies: List[Either[Exception, Json]] = streamedMessages.map { message =>
          ungzip(message.getBody).map(decodeUtf8).flatMap(parse)
        }
        _ = assert(messages.map(v => Right(Json.fromString(v))) === streamedBodies)
      } yield {}
    }
  }

  test("consumer requeue on consume") {
    val messages = (1 to 2).map(i => s"hello from fs2-rabbit-$i").toList

    TestUtils.channelWithPublishedMessage(connection)(messages) { case (declaration, consumer, queue) =>
      for {
        deliveryStream <- consumer.deliveryStream(queue.queueName, prefetchCount = 1)
        (consumerTag, stream) = deliveryStream
        declareOk <- declaration.queueDeclare(queue)
        _ = assert(declareOk.getMessageCount == 1)
        deliveries <- stream.take(1).compile.toList
        delivery = deliveries.head
        _ <- consumer.basicReject(DeliveryTag(delivery.getEnvelope.getDeliveryTag), requeue = true)
        declareOk2 <- declaration.queueDeclare(queue)
        _ = assert(declareOk2.getMessageCount == 1)
      } yield {}
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    closeIO.unsafeRunSync()
  }
}
