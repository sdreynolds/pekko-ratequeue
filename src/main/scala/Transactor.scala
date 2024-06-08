package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.util.{
  Failure,
  Success
}
import java.util.UUID
import org.apache.pekko.util.Timeout
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import org.apache.pekko.actor.Cancellable

object Transactor {
  sealed trait Command[T]
  case class Enqueue[T](identifier: String, jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  case class CompleteTransaction[T](transactionid: UUID) extends Command[T]
  case class FailedTransaction[T](transactionId: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartTransaction[T](replyTo: ActorRef[Response[T]], uuid: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartEmpty[T](replyTo: ActorRef[Response[T]]) extends Command[T]


  sealed trait Response[T]
  case class Transaction[T](id: String, identifier: String, jsonObject: T) extends Response[T]
  case class Empty[T]() extends Response[T]

  def apply[T](transactionDuration: FiniteDuration = FiniteDuration(10, "seconds")): Behavior[Command[T]] = {
    Behaviors.setup {context =>
      val queue = context.spawnAnonymous(QueueOfQueues[T]())
      transactorBehavior(Map.empty, queue, transactionDuration)
    }
  }

  private def transactorBehavior[T](activeTransactions: Map[UUID, Cancellable], queue: ActorRef[QueueOfQueues.Command[T]], transactionDuration: FiniteDuration): Behavior[Command[T]] = {
    Behaviors.receive((context, message) => {
      message match {
        case Enqueue(identifier, jsonObject) => {
          queue ! QueueOfQueues.Enqueue(identifier, jsonObject)
          Behaviors.same
        }
        case Dequeue(replyTo) => {
          context.ask(queue, QueueOfQueues.Dequeue[T].apply)(attempt => {
            attempt match {
              case Success(QueueOfQueues.NextEvent(json, identifier)) => StartTransaction(replyTo, UUID.randomUUID(), identifier, json)
              case Success(QueueOfQueues.Empty()) => StartEmpty(replyTo)
              case Failure(ex) => StartEmpty(replyTo)
            }
          })(Timeout(3, TimeUnit.SECONDS))

          Behaviors.same
        }
        case FailedTransaction(uuid, identifier, jsonObject) => {
          // @TODO: this is a problem, we don't know the phone number
          queue ! QueueOfQueues.Enqueue(identifier, jsonObject)

          // Canceling a already fired Cancelable is ok
          activeTransactions.get(uuid).map(_.cancel())
          transactorBehavior(activeTransactions - uuid, queue, transactionDuration)
        }
        case CompleteTransaction(uuid) => {
          activeTransactions.get(uuid).map(_.cancel())
          transactorBehavior(activeTransactions - uuid, queue, transactionDuration)
        }

        case StartTransaction(replyTo, uuid, identifier, jsonObject) => {
          replyTo ! Transaction(uuid.toString(), identifier, jsonObject)

          transactorBehavior(activeTransactions +
            (uuid -> context.scheduleOnce(transactionDuration, context.self, FailedTransaction(uuid, identifier, jsonObject))),
            queue, transactionDuration)
        }
        case StartEmpty(replyTo) => {
          replyTo ! Empty()
          Behaviors.same
        }
      }
    })
  }
}
