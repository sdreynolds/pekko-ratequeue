package com.github.sdreynolds.ratequeue

import scala.concurrent.duration._

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import com.github.sdreynolds.ratequeue.QueueOfQueues.{
  Enqueue,
  Dequeue,
  NextEvent,
  Empty,
  Response
}
import org.apache.pekko.actor.testkit.typed.scaladsl.TestInbox
import org.apache.pekko.actor.testkit.typed.javadsl.FishingOutcomes

class QueueOfQueuesSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "QueueOfQueues" must {
    "When empty return empty" in {
      val queue = testKit.spawn(QueueOfQueues[String]("abc"))
      val inbox = testKit.createTestProbe[Response[String]]()
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }

    "Return the payload added to the queue" in {
      val queue = testKit.spawn(QueueOfQueues[String]("abc"))
      val jsonPayload = "{\"awesome\": \"yes\"}"

      queue ! Enqueue("18009999999", jsonPayload)

      val inbox = testKit.createTestProbe[Response[String]]()
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(NextEvent(json = jsonPayload, identifier = "18009999999" ))

      // @TODO: peek to see if child is stopped
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }
    "Return multiple payloads added to the queue" in {
      val queue = testKit.spawn(QueueOfQueues[String]("abci"))
      val jsonPayload = "{\"awesome\": \"yes\"}"

      queue ! Enqueue("18009999999", jsonPayload)
      queue ! Enqueue("18009999999", jsonPayload)

      val inbox = testKit.createTestProbe[Response[String]]()
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(NextEvent(json = jsonPayload, identifier = "18009999999" ))

      queue ! Dequeue(inbox.ref)

      // Should not return for another second
      val responses = inbox.fishForMessage(1000.millis)(m => {
        m match
        case Empty() => {
          queue ! Dequeue(inbox.ref)
          FishingOutcomes.continueAndCollect()
        }
        case _: NextEvent[String] => FishingOutcomes.complete()
      })

      assert(responses.length > 1, "Responses must have at least one Empty response")

      // @TODO: peek to see if child is stopped
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }
  }
}
