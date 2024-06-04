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
  sealed trait Command
  case class Enqueue(identifier: String, jsonObject: String) extends Command
  case class Dequeue(replyTo: ActorRef[Response]) extends Command
  private case class Event(replyTo: ActorRef[Response], json: String, identifier: String) extends Command
  private case class LastEvent(replyTo: ActorRef[Response], json: String, identifier: String) extends Command
  private case class FailedDequeue(replyTo: ActorRef[Response], identifier: String) extends Command

  sealed trait Response
  case class NextEvent(json: String, identifier: String) extends Response
  case class Empty() extends Response

  class QueueItem[T](val identifier: String, val jsonObject: T, val queueRelease: Instant)  extends Comparable[QueueItem[T]] {
    def compareTo(other: QueueItem[T]) = queueRelease.compareTo(other.queueRelease)
  }

  def apply(): Behavior[Command] = {
    val queue = new PriorityQueue[QueueItem[String]]()
    queueBehavior(queue)
  }

  private def queueBehavior[T](queue: PriorityQueue[QueueItem[String]]): Behavior[Command] = {
    Behaviors.receive((context, msg) => {
      msg match {
        case Enqueue(identifier, jsonObject: String) => {

          context.child(identifier) match {
            case Some(childRawActor) => {
              val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command]]
              childActor ! PhoneQueue.Enqueue(jsonObject)
            }
            case None => {
              val newQueue = context.spawn(PhoneQueue[String](), identifier)
              val queueItem = new QueueItem[String](identifier, jsonObject, Instant.now())
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
                val childActor = childRawActor.asInstanceOf[ActorRef[PhoneQueue.Command]]
                context.ask(childActor, PhoneQueue.Dequeue.apply)((attempt: Try[PhoneQueue.Response]) =>
                  attempt match {
                    case Success(PhoneQueue.NextEvent(json: String)) => Event(replyTo, json, next.identifier)
                    case Success(PhoneQueue.NextEventAndEmpty(json: String)) => LastEvent(replyTo, json, next.identifier)
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
