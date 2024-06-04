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
  sealed trait Command
  case class Enqueue(identifier: String, jsonObject: String) extends Command
  case class Dequeue(replyTo: ActorRef[Response]) extends Command
  case class CompleteTransaction(transactionid: UUID) extends Command
  case class FailedTransaction(transactionId: UUID, identifier: String, jsonObject: String) extends Command
  private case class StartTransaction(replyTo: ActorRef[Response], uuid: UUID, identifier: String, jsonObject: String) extends Command
  private case class StartEmpty(replyTo: ActorRef[Response]) extends Command


  sealed trait Response
  case class Transaction(id: String, identifier: String, jsonObject: String) extends Response
  case class Empty() extends Response

  def apply(): Behavior[Command] = {
    Behaviors.setup {context =>
      val queue = context.spawnAnonymous(QueueOfQueues())
      transactorBehavior(Map.empty, queue)
    }
  }

  private def transactorBehavior(activeTransactions: Map[UUID, Cancellable], queue: ActorRef[QueueOfQueues.Command]): Behavior[Command] = {
    Behaviors.receive((context, message) => {
      message match {
        case Enqueue(identifier, jsonObject) => {
          queue ! QueueOfQueues.Enqueue(identifier, jsonObject)
          Behaviors.same
        }
        case Dequeue(replyTo) => {
          context.ask(queue, QueueOfQueues.Dequeue.apply)(attempt => {
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
          transactorBehavior(activeTransactions - uuid, queue)
        }
        case CompleteTransaction(uuid) => {
          activeTransactions.get(uuid).map(_.cancel())
          transactorBehavior(activeTransactions - uuid, queue)
        }

        case StartTransaction(replyTo, uuid, identifier, jsonObject) => {
          replyTo ! Transaction(uuid.toString(), identifier, jsonObject)

          transactorBehavior(activeTransactions +
            (uuid -> context.scheduleOnce(FiniteDuration(10, "seconds"), context.self, FailedTransaction(uuid, identifier, jsonObject))),
            queue)
        }
        case StartEmpty(replyTo) => {
          replyTo ! Empty()
          Behaviors.same
        }
      }
    })
  }
}
