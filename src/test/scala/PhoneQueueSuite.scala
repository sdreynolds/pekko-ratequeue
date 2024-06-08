package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import com.github.sdreynolds.ratequeue.PhoneQueue.Enqueue
import com.github.sdreynolds.ratequeue.PhoneQueue.Response
import com.github.sdreynolds.ratequeue.PhoneQueue.Dequeue
import com.github.sdreynolds.ratequeue.PhoneQueue.NextEvent
import com.github.sdreynolds.ratequeue.PhoneQueue.NextEventAndEmpty
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit

class PhoneQueueSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "PhoneQueue" must {
    "Return the Payload for the request"  in {
      val jsonPayload = "{\"awesome\": \"yes\"}"
      val queue = testKit.spawn(PhoneQueue[String]())
      queue ! Enqueue(jsonPayload)
      val dequeueProbe = testKit.createTestProbe[Response[String]]()

      queue ! Dequeue(dequeueProbe.ref)
      dequeueProbe.expectMessage(NextEventAndEmpty(jsonPayload))
    }

    "Return both payloads from the queue" in {
      val jsonPayload = "{\"awesome\": \"yes\"}"
      val queue = testKit.spawn(PhoneQueue[String]())
      queue ! Enqueue(jsonPayload)
      queue ! Enqueue(jsonPayload)
      val dequeueProbe = testKit.createTestProbe[Response[String]]()

      queue ! Dequeue(dequeueProbe.ref)
      dequeueProbe.expectMessage(NextEvent(jsonPayload))

      queue ! Dequeue(dequeueProbe.ref)
      dequeueProbe.expectMessage(NextEventAndEmpty(jsonPayload))
    }
  }
}
