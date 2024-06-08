package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.immutable.Queue
import org.apache.pekko.actor.typed.ActorRef

object PhoneQueue {
  sealed trait Command[T]
  case class Enqueue[T](jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]

  sealed trait Response[T]
  case class NextEvent[T](json: T) extends Response[T]
  case class NextEventAndEmpty[T](json: T) extends Response[T]

  def apply[T](): Behavior[Command[T]] = queueState(Queue[T]())

  private def queueState[T](queue: Queue[T]): Behavior[Command[T]] =
    Behaviors.receiveMessage(msg => {
      msg match {
        case Enqueue(json) => queueState(queue.enqueue(json))
        case Dequeue(replyTo) => {
          val (next, newQueue) = queue.dequeue
          if newQueue.isEmpty then
            replyTo ! NextEventAndEmpty(next)
            Behaviors.stopped
          else
            replyTo ! NextEvent(next)
            queueState(newQueue)
        }
      }
    })
}
