package com.github.sdreynolds.ratequeue

import com.github.sdreynolds.ratequeue.PhoneQueue.NextEventAndEmpty
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.EventSourcedBehavior
import org.apache.pekko.util.Timeout

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.mutable.PriorityQueue
import scala.util.Failure
import scala.util.Success
import scala.util.Try


object QueueOfQueues {
  sealed trait Command[T]
  case class Enqueue[T](identifier: String, jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  private case class Payload[T](replyTo: ActorRef[Response[T]], json: T, identifier: String) extends Command[T]
  private case class LastPayload[T](replyTo: ActorRef[Response[T]], json: T, identifier: String) extends Command[T]
  private case class FailedDequeue[T](replyTo: ActorRef[Response[T]], identifier: String) extends Command[T]

  sealed trait Event[T]
  private case class CreatePhoneQueue[T](queueItem: QueueItem[T]) extends Event[T]
  private case class DequeueEvent[T]() extends Event[T]
  private case class ReEnqueuePhoneQueue[T](queueItem: QueueItem[T]) extends Event[T]

  sealed trait Response[T]
  case class NextEvent[T](json: T, identifier: String) extends Response[T]
  case class Empty[T]() extends Response[T]

  class QueueItem[T](val identifier: String, val queueRelease: Instant)  extends Comparable[QueueItem[T]] {
    def compareTo(other: QueueItem[T]) = queueRelease.compareTo(other.queueRelease)
  }

  final case class State[T](queue: PriorityQueue[QueueItem[T]])

  def apply[T](shardId: String): Behavior[Command[T]] = {
    Behaviors.setup { context =>
      EventSourcedBehavior(
        persistenceId = PersistenceId.ofUniqueId(shardId),
        emptyState = State(queue = PriorityQueue[QueueItem[T]]()),
        commandHandler = priorityCommandHandler(context),
        eventHandler = eventHandler
      )
    }
  }

  private def eventHandler[T](state: State[T], event: Event[T]): State[T] = {
    event match
    case CreatePhoneQueue(queueItem) => {
      state.queue += queueItem
      state
    }
    case DequeueEvent() => {
      state.queue.dequeue()
      state
    }
    case ReEnqueuePhoneQueue(queueItem) => {
      state.queue += queueItem
      state
    }
  }

  private def priorityCommandHandler[T](context: ActorContext[Command[T]])(state: State[T], command: Command[T]): Effect[Event[T], State[T]] = {
    command match
    case Enqueue(identifier, jsonObject) => {
      context.child(identifier) match {
        case Some(childRawActor) => {
          val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command[T]]]
          Effect.none
          .thenReply(childActor)(_ => PhoneQueue.Enqueue[T](jsonObject))
        }
        case None => {
          Effect.persist(CreatePhoneQueue(QueueItem(identifier, Instant.now())))
            .thenRun(_ => {
              val child = context.spawn(PhoneQueue[T](identifier), identifier)
              child ! PhoneQueue.Enqueue(jsonObject)
            })
        }
      }
    }
    case Dequeue(replyTo) => {
      state.queue.headOption match
      case None => Effect.none.thenReply(replyTo)(_ => Empty())
      case Some(next) if next.queueRelease.isAfter(Instant.now()) => Effect.none
          .thenReply(replyTo)(_ => Empty())

      case Some(next) => {
        context.child(next.identifier) match {
          case Some(childRawActor) => {
            Effect.persist(DequeueEvent())
              .thenRun(_ => {
                val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command[T]]]
                context.ask(childActor, PhoneQueue.Dequeue[T].apply)((attempt: Try[PhoneQueue.Response[T]]) =>
                  attempt match {
                    case Success(PhoneQueue.NextEvent(json)) => Payload(replyTo, json, next.identifier)
                    case Success(PhoneQueue.NextEventAndEmpty(json)) => LastPayload(replyTo, json, next.identifier)
                    case Failure(ex) => FailedDequeue(replyTo, next.identifier)
                  }
                )(Timeout(3, TimeUnit.SECONDS))
              })
          }
          case None => {
            context.log.error("lost phone number queue for identifer {}", next)
            Effect.none.thenReply(context.self)(_ => FailedDequeue(replyTo, next.identifier))
          }
        }
      }
    }
    case LastPayload(replyTo, json, identifier) => Effect.none.thenReply(replyTo)(_ => NextEvent(json, identifier))

    case Payload(replyTo, json, identifier) => Effect.persist(
        ReEnqueuePhoneQueue(QueueItem(identifier, Instant.now().plusSeconds(1)))
    ).thenReply(replyTo)(_ => NextEvent(json, identifier))

    case FailedDequeue(replyTo, identifier) => Effect.persist(
      ReEnqueuePhoneQueue(QueueItem(identifier, Instant.now())))
        .thenReply(replyTo)(_ => Empty())
  }
}
