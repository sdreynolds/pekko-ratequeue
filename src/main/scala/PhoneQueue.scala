package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.persistence.typed.scaladsl.EventSourcedBehavior

import scala.collection.immutable.Queue

object PhoneQueue {
  sealed trait Command[T]
  case class Enqueue[T](jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]

  sealed trait Event[T]
  case class EnqueueEvent[T](jsonObject: T) extends Event[T]
  case class DequeueEvent[T]() extends Event[T]

  sealed trait Response[T]
  case class NextEvent[T](json: T) extends Response[T]
  case class NextEventAndEmpty[T](json: T) extends Response[T]

  final case class State[T](queue: Queue[T] = Queue())

  def apply[T](name: String): Behavior[Command[T]] = queueState(name, Queue[T]())

  private def queueState[T](name: String, queue: Queue[T]): Behavior[Command[T]] =
    EventSourcedBehavior[Command[T], Event[T], State[T]] (
      persistenceId = PersistenceId.ofUniqueId(name),
      emptyState = State(),
      commandHandler = queueCommandHandler,
      eventHandler = eventHandler
    )

  private def queueCommandHandler[T](state: State[T], command: Command[T]): Effect[Event[T], State[T]] = {
    command match {
      case Enqueue(obj) => Effect.persist(EnqueueEvent(obj))
      case Dequeue(replyTo) =>
        val (head, tail) = state.queue.dequeue
        if tail.isEmpty then
          Effect.persist(DequeueEvent())
            .thenStop()
            .thenReply(replyTo)(_ => NextEventAndEmpty(head))
        else
          Effect.persist(DequeueEvent()).thenReply(replyTo)(_ => NextEvent(head))
    }
  }

  private def eventHandler[T](state: State[T], event: Event[T]): State[T] = {
    event match {
      case EnqueueEvent(jsonObject) => state.copy(queue = state.queue :+ jsonObject)
      case DequeueEvent() => {
        val (_, newQueue) = state.queue.dequeue
        state.copy(queue = newQueue)
      }
    }
  }
}
