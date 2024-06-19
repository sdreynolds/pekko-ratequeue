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
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.EventSourcedBehavior
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.actor.typed.scaladsl.ActorContext

object Transactor {
  sealed trait Command[T]
  case class Enqueue[T](identifier: String, jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  case class CompleteTransaction[T](transactionid: UUID) extends Command[T]
  case class FailedTransaction[T](transactionId: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartTransaction[T](replyTo: ActorRef[Response[T]], uuid: UUID, identifier: String, jsonObject: T) extends Command[T]
  private case class StartEmpty[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  private case class CleanTimedoutTransactions[T]() extends Command[T]

  sealed trait Event[T]
  private case class TransactionStart[T](txn: Transaction[T]) extends Event[T]
  private case class RemoveTransactions[T](txns: Iterable[Transaction[T]]) extends Event[T]


  sealed trait Response[T]
  case class Transaction[T](id: String, identifier: String, jsonObject: T, expires: Instant) extends Response[T]
  case class Empty[T]() extends Response[T]

  private final case class State[T](activeTransactions: Map[UUID, Transaction[T]])

  def apply[T](transactionDuration: FiniteDuration = FiniteDuration(10, "seconds")): Behavior[Command[T]] = {
    Behaviors.setup {context =>
      Behaviors.withTimers(factory => {
        val queue = context.spawnAnonymous(QueueOfQueues[T]("abc"))
        factory.startTimerWithFixedDelay(CleanTimedoutTransactions(), transactionDuration / 4)

        EventSourcedBehavior(
          persistenceId = PersistenceId.ofUniqueId("transactor1"),
          emptyState = State(Map.empty),
          commandHandler = transactorCommandHandler(transactionDuration, queue, context),
          eventHandler = eventHandler
        )
      })
    }
  }

  private def transactorCommandHandler[T](transactionDuration: FiniteDuration, queue: ActorRef[QueueOfQueues.Command[T]], context: ActorContext[Command[T]])
    (state: State[T], command: Command[T]): Effect[Event[T], State[T]] = {
    command match
    case Enqueue(identifier, jsonObject) => Effect.none.thenReply(queue)(_ => QueueOfQueues.Enqueue(identifier, jsonObject))
    case Dequeue(replyTo) => {
      Effect.none.thenRun(_ => {
        context.ask(queue, QueueOfQueues.Dequeue[T].apply)(attempt => {
          attempt match {
            case Success(QueueOfQueues.NextEvent(json, identifier)) => StartTransaction(replyTo, UUID.randomUUID(), identifier, json)
            case Success(QueueOfQueues.Empty()) => StartEmpty(replyTo)
            case Failure(ex) => StartEmpty(replyTo)
          }
        })(Timeout(3, TimeUnit.SECONDS))
      })
    }
    case CompleteTransaction(transactionId) => Effect.persist(RemoveTransactions(
      List(state.activeTransactions(transactionId))))

    case FailedTransaction(transactionId, identifier, jsonObject) => {
      val txn = state.activeTransactions(transactionId)
      Effect.persist(RemoveTransactions(List(txn)))
        .thenRun(_ => queue ! QueueOfQueues.Enqueue(txn.identifier, txn.jsonObject))
    }

    case StartTransaction(replyTo, uuid, identifier, jsonObject) => {
      val txn = Transaction(uuid.toString(), identifier, jsonObject, Instant.now().plusMillis(transactionDuration.toMillis))
      Effect.persist(TransactionStart(txn)).thenReply(replyTo)(_ => txn)
    }
    case StartEmpty(replyTo) => Effect.none.thenReply(replyTo)(_ => Empty())

    case CleanTimedoutTransactions() => {
      val current = Instant.now()
      val expiredTransactions = state.activeTransactions
        .filter((id, txn) => txn.expires.isAfter(current))

      Effect.persist(RemoveTransactions(expiredTransactions.values))
        .thenRun(_ => expiredTransactions.values.foreach(txn => queue ! QueueOfQueues.Enqueue(txn.identifier, txn.jsonObject)))
    }

  }

  private def eventHandler[T](state: State[T], event: Event[T]): State[T] = {
    event match
    case TransactionStart(txn) => State(activeTransactions = state.activeTransactions + (UUID.fromString(txn.id) -> txn))
    case RemoveTransactions(txns) => State(activeTransactions = state.activeTransactions -- txns.map(txn => UUID.fromString(txn.id)))
  }
}
