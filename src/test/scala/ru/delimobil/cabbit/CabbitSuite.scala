package ru.delimobil.cabbit

import java.util.UUID

import cats.Parallel
import cats.effect.ContextShift
import cats.effect.Effect
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync
import cats.effect.Timer
import cats.effect.syntax.effect._
import cats.syntax.applicativeError._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.parallel._
import cats.syntax.semigroupal._
import cats.syntax.traverse._
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.AMQP.Queue
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.GetResponse
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import ru.delimobil.cabbit.CabbitSuite._
import ru.delimobil.cabbit.algebra.ContentEncoding._
import ru.delimobil.cabbit.algebra._
import ru.delimobil.cabbit.config.declaration._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class CabbitSuite extends AnyFunSuite with BeforeAndAfterAll {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO] = IO.timer(scala.concurrent.ExecutionContext.global)

  private val sleep = timer.sleep(50.millis)

  private var container: RabbitContainer = _
  private var rabbitUtils: RabbitUtils[IO] = _
  private var closeIO: IO[Unit] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    RabbitContainer
      .make[IO]
      .mproduct(cont => RabbitContainer.makeConnection[IO](cont).flatMap(make[IO]))
      .allocated
      .flatTap {
        case ((cont, utils), closeAction) =>
          IO.delay {
            container = cont
            rabbitUtils = utils
            closeIO = closeAction
          }
      }
      .unsafeRunSync
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    closeIO.unsafeRunSync()
  }

  test("queue declaration returns state") {
    rabbitUtils.useRandomlyDeclared {
      case (queue, _) =>
        for {
          declareResult <- rabbitUtils.declare(queue)
          _ = assert(declareResult.getQueue == queue.queueName.name)
          _ = assert(declareResult.getConsumerCount == 0)
          _ = assert(declareResult.getMessageCount == 0)
        } yield {}
      }
  }

  test("publisher publishes") {
    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(List("hello from fs2-rabbit"), bind)
          _ <- sleep
          declareResult <- rabbitUtils.declare(queue)
          _ = assert(declareResult.getMessageCount == 1)
        } yield {}
      }
  }

  test("consumer `basicGet` `autoAck = true`") {
    val message = "hello from fs2-rabbit"

    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(List(message), bind)
          _ <- sleep
          response <- rabbitUtils.basicGet(bind.queueName, autoAck = true)
          declareOk <- rabbitUtils.declare(queue)
          bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
          _ = assert(bodyResponse === Right(Json.fromString(message)))
          _ = assert(declareOk.getConsumerCount === 0)
          _ = assert(declareOk.getMessageCount === 0)
        } yield {}
      }
  }

  test("consumer rejects `basicGet`, `requeue = true`") {
    val message = "hello from fs2-rabbit"

    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(List(message), bind)
          _ <- sleep
          response <- rabbitUtils.basicGet(bind.queueName, autoAck = false)
          _ <- rabbitUtils.basicReject(DeliveryTag(response.getEnvelope.getDeliveryTag), requeue = true)
          _ <- sleep
          declareOk <- rabbitUtils.declare(queue)
          bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
          _ = assert(bodyResponse === Right(Json.fromString(message)))
          _ = assert(declareOk.getConsumerCount === 0)
          _ = assert(declareOk.getMessageCount === 1)
        } yield {}
      }
  }

  test("consumer rejects `basicGet` `requeue = false`") {
    val message = "hello from fs2-rabbit"

    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(List(message), bind)
          _ <- sleep
          response <- rabbitUtils.basicGet(bind.queueName, autoAck = false)
          _ <- rabbitUtils.basicReject(DeliveryTag(response.getEnvelope.getDeliveryTag), requeue = false)
          _ <- sleep
          declareOk <- rabbitUtils.declare(queue)
          bodyResponse = ungzip(response.getBody).map(decodeUtf8).flatMap(parse)
          _ = assert(bodyResponse === Right(Json.fromString(message)))
          _ = assert(declareOk.getConsumerCount === 0)
          _ = assert(declareOk.getMessageCount === 0)
        } yield {}
      }
  }

  test("consumer consumes") {
    val maxMessages = 1000
    val prefetched = 51
    val messages = List.fill(maxMessages)(Random.nextString(Random.nextInt(100)))

    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(messages, bind)
          (_, stream) <- rabbitUtils.deliveryStream(bind.queueName, prefetched)
          _ <- sleep
          declareOk <- rabbitUtils.declare(queue)
          deliveries <- rabbitUtils.readAckN(stream, maxMessages)
          bodies = deliveries.map(msg => ungzip(msg.getBody).map(decodeUtf8).flatMap(parse))
          _ = assert(declareOk.getConsumerCount == 1)
          _ = assert(declareOk.getMessageCount == (maxMessages - prefetched))
          _ = assert(messages.map(v => Right(Json.fromString(v))) === bodies)
        } yield {}
      }
  }

  ignore("consumer completes on cancel, msg gets requeued") {
    val messages = (1 to 2).map(i => s"hello from fs2-rabbit-$i").toList

    rabbitUtils.useRandomlyDeclared {
      case (queue, bind) =>
        for {
          _ <- rabbitUtils.publish(messages, bind)
          (tag, stream) <- rabbitUtils.deliveryStream(bind.queueName, 1)
          _ <- rabbitUtils.cancel(tag)
          _ <- sleep
          deliveriesEither <- IO.race(stream.compile.toList, IO.sleep(300.millis))
          declareOk2 <- rabbitUtils.declare(queue)
          _ = assert(deliveriesEither.isLeft)
          _ = assert(declareOk2.getMessageCount == 2)
        } yield {}
      }
  }

  test("Stream completes on queue removal") {
    rabbitUtils.declareAcquire
      .flatMap {
        case (_, bind) =>
          for {
            (_, stream) <- rabbitUtils.deliveryStream(bind.queueName, 1)
            _ <- rabbitUtils.declareRelease(bind)
            _ <- sleep
            deliveriesEither <- IO.race(stream.compile.toList, IO.sleep(300.millis))
            _ = assert(deliveriesEither.isLeft)
          } yield {}
      }
      .unsafeRunSync()
  }

  test("two exclusive queues on one connection should work") {
    RabbitContainer
      .makeConnection[IO](container)
      .flatMap(conn => (conn.createChannelDeclaration, conn.createChannelDeclaration).tupled)
      .use { case (channel1, channel2) =>
        rabbitUtils.declareExclusive(channel1, channel2)
          .map { case (result1, result2) => assert(result1.isRight && result2.isRight) }
      }
      .unsafeRunSync()
  }

  test("only one of two exclusive queues on different connections should work") {
    val connRes = RabbitContainer.makeConnection[IO](container)
    (connRes, connRes)
      .tupled
      .mproduct { case (conn1, conn2) => (conn1.createChannelDeclaration, conn2.createChannelDeclaration).tupled }
      .use { case ((conn1, conn2), (channel1, channel2)) =>
        rabbitUtils.declareExclusive(channel1, channel2)
          .product((channel1.isOpen, channel2.isOpen, conn1.isOpen, conn2.isOpen).tupled)
          .map { case ((result1, result2), (ch1Open, ch2Open, conn1Open, conn2Open)) =>
            assert(result1.isRight ^ result2.isRight)
            assert(ch1Open ^ ch2Open)
            assert(conn1Open && conn2Open)
          }
      }
      .unsafeRunSync()
  }
}

private object CabbitSuite {

  final class RabbitUtils[F[_]: Effect: Parallel](
    declaration: ChannelDeclaration[F],
    consumer: ChannelConsumer[F],
    publisher: ChannelPublisher[F]
  ) {

    import ru.delimobil.cabbit.algebra.BodyEncoder.instances.jsonGzip

    def basicGet(queue: QueueName, autoAck: Boolean): F[GetResponse] =
      consumer.basicGet(queue, autoAck)

    def basicReject(tag: DeliveryTag, requeue: Boolean): F[Unit] =
      consumer.basicReject(tag, requeue)

    def declare(queue: QueueDeclaration): F[AMQP.Queue.DeclareOk] =
      declaration.queueDeclare(queue)

    def cancel(tag: ConsumerTag): F[Unit] =
      consumer.basicCancel(tag)

    def deliveryStream(queue: QueueName, prefetch: Int): F[(ConsumerTag, Stream[F, Delivery])] =
      consumer.deliveryStream(queue, prefetch)

    def readAckN(stream: Stream[F, Delivery], n: Int): F[List[Delivery]] =
      stream
        .evalTap(d => consumer.basicAck(DeliveryTag(d.getEnvelope.getDeliveryTag), multiple = false))
        .take(n)
        .compile
        .toList

    def publish(messages: List[String], bind: BindDeclaration): F[List[Unit]] =
      messages.traverse(publishOne(bind.exchangeName, bind.routingKey, _))

    private def publishOne(exchange: ExchangeName, key: RoutingKey, msg: String) =
      publisher.basicPublish(exchange, key, new AMQP.BasicProperties, mandatory = true, msg)

    def declareAcquire: F[(QueueDeclaration, BindDeclaration)] =
      for {
        declarations <- Sync[F].delay(getDeclarations(UUID.randomUUID()))
        (exchange, queue, bind) = declarations
        _ <- declaration.exchangeDeclare(exchange)
        _ <- declaration.queueDeclare(queue)
        _ <- declaration.queueBind(bind)
      } yield (queue, bind)

    def declareRelease(bind: BindDeclaration): F[Unit] = {
      val bindingIO = declaration.queueUnbind(bind)
      val exchangeIO = declaration.exchangeDelete(bind.exchangeName)
      val queueIO = declaration.queueDelete(bind.queueName)
      bindingIO <* exchangeIO <* queueIO
    }

    def useRandomlyDeclared(testFunc: (QueueDeclaration, BindDeclaration) => F[Unit]): Unit =
      Resource.make(declareAcquire) { case (_, bind) => declareRelease(bind) }
        .use { case (queue, bind) => testFunc(queue, bind) }
        .toIO
        .unsafeRunSync()

    def declareExclusive(
      channel1: ChannelDeclaration[F],
      channel2: ChannelDeclaration[F]
    ): F[(Either[Throwable, Queue.DeclareOk], Either[Throwable, Queue.DeclareOk])] =
      Sync[F].delay(UUID.randomUUID())
        .flatMap { uuid =>
          val queue = QueueDeclaration(
            QueueName(uuid.toString),
            DurableConfig.NonDurable,
            ExclusiveConfig.Exclusive,
            AutoDeleteConfig.AutoDelete,
            Map.empty
          )

          (channel1.queueDeclare(queue).attempt, channel2.queueDeclare(queue).attempt).parTupled
        }

    private def getDeclarations(uuid: UUID): (ExchangeDeclaration, QueueDeclaration, BindDeclaration) = {
      val exchange =
        ExchangeDeclaration(
          ExchangeName(s"the-exchange-$uuid"),
          BuiltinExchangeType.DIRECT,
          DurableConfig.NonDurable,
          AutoDeleteConfig.NonAutoDelete,
          InternalConfig.NonInternal,
          Map.empty
        )

      val queue =
        QueueDeclaration(
          QueueName(s"the-queue-$uuid"),
          DurableConfig.NonDurable,
          ExclusiveConfig.NonExclusive,
          AutoDeleteConfig.NonAutoDelete,
          Map.empty
        )

      val binding = BindDeclaration(queue.queueName, exchange.exchangeName, RoutingKey("the-key"))

      (exchange, queue, binding)
    }
  }

  def make[F[_]: Effect: Parallel](conn: Connection[F]): Resource[F, RabbitUtils[F]] =
    for {
      declaration <- conn.createChannelDeclaration
      consumer <- conn.createChannelConsumer
      publisher <- conn.createChannelPublisher
    } yield new RabbitUtils(declaration, consumer, publisher)
}
