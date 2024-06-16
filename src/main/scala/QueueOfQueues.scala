package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import java.util.PriorityQueue
import java.time.Instant
import com.github.sdreynolds.ratequeue.PhoneQueue.NextEventAndEmpty

import scala.util.{
  Failure,
  Success
}
import org.apache.pekko.util.Timeout
import java.util.concurrent.TimeUnit
import scala.util.Try


object QueueOfQueues {
  sealed trait Command[T]
  case class Enqueue[T](identifier: String, jsonObject: T) extends Command[T]
  case class Dequeue[T](replyTo: ActorRef[Response[T]]) extends Command[T]
  private case class Event[T](replyTo: ActorRef[Response[T]], json: T, identifier: String) extends Command[T]
  private case class LastEvent[T](replyTo: ActorRef[Response[T]], json: T, identifier: String) extends Command[T]
  private case class FailedDequeue[T](replyTo: ActorRef[Response[T]], identifier: String) extends Command[T]

  sealed trait Response[T]
  case class NextEvent[T](json: T, identifier: String) extends Response[T]
  case class Empty[T]() extends Response[T]

  class QueueItem[T](val identifier: String, val jsonObject: T, val queueRelease: Instant)  extends Comparable[QueueItem[T]] {
    def compareTo(other: QueueItem[T]) = queueRelease.compareTo(other.queueRelease)
  }

  def apply[T](): Behavior[Command[T]] = {
    val queue = new PriorityQueue[QueueItem[T]]()
    queueBehavior(queue)
  }

  private def queueBehavior[T](queue: PriorityQueue[QueueItem[T]]): Behavior[Command[T]] = {
    Behaviors.receive((context, msg) => {
      msg match {
        case Enqueue(identifier, jsonObject) => {

          context.child(identifier) match {
            case Some(childRawActor) => {
              val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command[T]]]
              childActor ! PhoneQueue.Enqueue(jsonObject)
            }
            case None => {
              val newQueue = context.spawn(PhoneQueue[T](identifier), identifier)
              val queueItem = new QueueItem(identifier, jsonObject, Instant.now())
              queue.add(queueItem)

              newQueue ! PhoneQueue.Enqueue(jsonObject)
            }
          }
          queueBehavior(queue)
        }
        case Dequeue(replyTo) => {
          if queue.isEmpty()
          then {
            replyTo ! Empty()
            Behaviors.same
          } else {

            val next = queue.poll()

            context.child(next.identifier) match {
              case Some(childRawActor) => {
                val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command[T]]]
                context.ask(childActor, PhoneQueue.Dequeue[T].apply)((attempt: Try[PhoneQueue.Response[T]]) =>
                  attempt match {
                    case Success(PhoneQueue.NextEvent(json)) => Event(replyTo, json, next.identifier)
                    case Success(PhoneQueue.NextEventAndEmpty(json)) => LastEvent(replyTo, json, next.identifier)
                    case Failure(ex) => FailedDequeue(replyTo, next.identifier)
                  }
                )(Timeout(3, TimeUnit.SECONDS))
              }
              case None => {
                context.log.error("lost phone number queue for identifer {}", next)
                context.self ! FailedDequeue(replyTo, next.identifier)
              }
            }
          }
          Behaviors.same
        }
        // Handling commands for Dequeue from the phone number queue
        case Event(replyTo, json, identifier) => {
          replyTo ! NextEvent(json, identifier)
          val reQueueItem = new QueueItem(identifier, json, Instant.now().minusSeconds(1))
          queue.add(reQueueItem)
          Behaviors.same
        }
        case LastEvent(replyTo, json, identifier) => {
          replyTo ! NextEvent(json, identifier)
          queueBehavior(queue)
        }
        case FailedDequeue(replyTo, identifier) =>
          replyTo ! Empty()
          queueBehavior(queue)
      }
    })
  }
}
