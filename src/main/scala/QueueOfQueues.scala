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
  trait Command
  case class Enqueue(identifier: String, jsonObject: String) extends Command
  case class Dequeue(replyTo: ActorRef[Response]) extends Command
  private case class Event(replyTo: ActorRef[Response], json: String, identifier: String) extends Command
  private case class LastEvent(replyTo: ActorRef[Response], json: String, identifier: String) extends Command
  private case class FailedDequeue(replyTo: ActorRef[Response], identifier: String) extends Command

  trait Response
  case class NextEvent(json: String) extends Response
  case class Empty() extends Response

  class QueueItem[T](val identifier: String, val jsonObject: T, val queueRelease: Instant)  extends Comparable[QueueItem[T]] {
    def compareTo(other: QueueItem[T]) = queueRelease.compareTo(other.queueRelease)
  }

  def apply(): Behavior[Command] = {
    val queue = new PriorityQueue[QueueItem[String]]()
    val activeQueues = Map[String, ActorRef[PhoneQueue.Command]]()
    queueBehavior(queue, activeQueues)
  }

  private def queueBehavior[T](queue: PriorityQueue[QueueItem[String]], activeQueues: Map[String, ActorRef[PhoneQueue.Command]]): Behavior[Command] = {
    Behaviors.receive((context, msg) => {
      msg match {
        case Enqueue(identifier, jsonObject: String) => {
          if activeQueues.contains(identifier) then {
            activeQueues(identifier) ! PhoneQueue.Enqueue(jsonObject)
            queueBehavior(queue, activeQueues)
          }
          else {
            val newQueue = context.spawn(PhoneQueue[String](), identifier)
            val queueItem = new QueueItem[String](identifier, jsonObject, Instant.now())
            queue.add(queueItem)

            newQueue ! PhoneQueue.Enqueue(jsonObject)

            queueBehavior(queue,
              activeQueues + (identifier -> newQueue))
          }
        }
        case Dequeue(replyTo) => {
          if queue.isEmpty()
          then {
            replyTo ! Empty()
            Behaviors.same
          } else {

            val next = queue.poll()
            context.ask(activeQueues(next.identifier), PhoneQueue.Dequeue.apply)((attempt: Try[PhoneQueue.Response]) =>
              // @TODO: weird that this says "may not be exhaustive"
              attempt match {
                case Success(PhoneQueue.NextEvent(json: String)) => Event(replyTo, json, next.identifier)
                case Success(PhoneQueue.NextEventAndEmpty(json: String)) => LastEvent(replyTo, json, next.identifier)
                case Failure(ex) => FailedDequeue(replyTo, next.identifier)
              })(Timeout(3, TimeUnit.SECONDS))

            Behaviors.same
          }
        }

        // Handling commands for Dequeue from the phone number queue
        case Event(replyTo, json, identifier) => {
          replyTo ! NextEvent(json)
          val reQueueItem = new QueueItem(identifier, json, Instant.now().minusSeconds(1))
          queue.add(reQueueItem)
          Behaviors.same
        }
        case LastEvent(replyTo, json, identifier) => {
          replyTo ! NextEvent(json)

          // remove from active queues
          // @TODO: probably need to list to all signals of child and remove if they stop instead of here
          queueBehavior(queue, activeQueues - identifier)
        }
        case FailedDequeue(replyTo, identifier) =>
          replyTo ! Empty()

          // remove from active queues
          // @TODO: Need to send the kill signal to this child? Will it just keep running?
          queueBehavior(queue, activeQueues - identifier)
      }
    })
  }
}
