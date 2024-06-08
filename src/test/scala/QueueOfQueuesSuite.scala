package com.github.sdreynolds.ratequeue

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

class QueueOfQueuesSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "QueueOfQueues" must {
    "When empty return empty" in {
      val queue = testKit.spawn(QueueOfQueues[String]())
      val inbox = testKit.createTestProbe[Response[String]]()
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }

    "Return the payload added to the queue" in {
      val queue = testKit.spawn(QueueOfQueues[String]())
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
      val queue = testKit.spawn(QueueOfQueues[String]())
      val jsonPayload = "{\"awesome\": \"yes\"}"

      queue ! Enqueue("18009999999", jsonPayload)
      queue ! Enqueue("18009999999", jsonPayload)

      val inbox = testKit.createTestProbe[Response[String]]()
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(NextEvent(json = jsonPayload, identifier = "18009999999" ))

      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(NextEvent(json = jsonPayload, identifier = "18009999999" ))

      // @TODO: peek to see if child is stopped
      queue ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }
  }
}
