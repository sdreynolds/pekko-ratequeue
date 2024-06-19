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
import scala.collection.immutable.Queue
import java.time.Instant

object Transactor {
  sealed trait Command[T]
  case class Enqueue[T](identifier: String, jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  case class CompleteTransaction[T](transactionid: UUID) extends Command[T]
  case class FailedTransaction[T](transactionId: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartTransaction[T](replyTo: ActorRef[Response[T]], uuid: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartEmpty[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  private case class CleanTimedoutTransactions[T]() extends Command[T]


  sealed trait Response[T]
  case class Transaction[T](id: String, identifier: String, jsonObject: T, expires: Instant) extends Response[T]
  case class Empty[T]() extends Response[T]

  def apply[T](transactionDuration: FiniteDuration = FiniteDuration(10, "seconds")): Behavior[Command[T]] = {
    Behaviors.setup {context =>
      Behaviors.withTimers(factory => {
        val queue = context.spawnAnonymous(QueueOfQueues[T]("abc"))
        factory.startTimerWithFixedDelay(CleanTimedoutTransactions(), transactionDuration / 4)

        transactorBehavior(Map.empty, queue, transactionDuration)
      })
    }
  }

  private def transactorBehavior[T](activeTransactions: Map[UUID, Transaction[T]], queue: ActorRef[QueueOfQueues.Command[T]], transactionDuration: FiniteDuration): Behavior[Command[T]] = {
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
          queue ! QueueOfQueues.Enqueue(identifier, jsonObject)
          transactorBehavior(activeTransactions - uuid, queue, transactionDuration)
        }
        case CompleteTransaction(uuid) => {
          transactorBehavior(activeTransactions - uuid, queue, transactionDuration)
        }

        case StartTransaction(replyTo, uuid, identifier, jsonObject) => {
          val txn = Transaction(uuid.toString(), identifier, jsonObject, Instant.now().plusMillis(transactionDuration.toMillis))
          replyTo ! txn

          transactorBehavior(activeTransactions + (uuid -> txn), queue, transactionDuration)
        }
        case StartEmpty(replyTo) => {
          replyTo ! Empty()
          Behaviors.same
        }
        case CleanTimedoutTransactions() => {
          val current = Instant.now()
          val expiredTransactions = activeTransactions
            .filter((id, txn) => txn.expires.isAfter(current))

          expiredTransactions.values.foreach(txn => queue ! QueueOfQueues.Enqueue(txn.identifier, txn.jsonObject))

          transactorBehavior(activeTransactions -- expiredTransactions.keys, queue, transactionDuration)
        }
      }
    })
  }
}
