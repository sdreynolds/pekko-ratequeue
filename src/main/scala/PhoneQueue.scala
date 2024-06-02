package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import scala.collection.immutable.Queue
import org.apache.pekko.actor.typed.ActorRef

object PhoneQueue {
  trait Command
  case class Enqueue[T](jsonObject: T) extends Command
  case class Dequeue(replyTo: ActorRef[Response]) extends Command

  trait Response
  case class NextEvent[T](json: T) extends Response
  case class NextEventAndEmpty[T](json: T) extends Response

  def apply[T](): Behavior[Command] = queueState(Queue[T]())

  private def queueState[T](queue: Queue[T]): Behavior[Command] =
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
