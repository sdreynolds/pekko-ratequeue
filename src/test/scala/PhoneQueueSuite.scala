package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import com.github.sdreynolds.ratequeue.PhoneQueue.Enqueue
import com.github.sdreynolds.ratequeue.PhoneQueue.Response
import com.github.sdreynolds.ratequeue.PhoneQueue.Dequeue
import com.github.sdreynolds.ratequeue.PhoneQueue.NextEvent
import com.github.sdreynolds.ratequeue.PhoneQueue.NextEventAndEmpty

class PhoneQueueSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "PhoneQueue" must {
    val queue = testKit.spawn(PhoneQueue())
    "Return the Payload for the request"  in {
      val jsonPayload = "{\"awesome\": \"yes\"}"

      queue ! Enqueue(jsonPayload)
      val dequeueProbe = testKit.createTestProbe[Response]()

      queue ! Dequeue(dequeueProbe.ref)
      dequeueProbe.expectMessage(NextEventAndEmpty(jsonPayload))
    }
  }
}
