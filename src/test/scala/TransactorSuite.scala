package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import com.github.sdreynolds.ratequeue.Transactor.{
  CompleteTransaction,
  Empty,
  Enqueue,
  Dequeue,
  FailedTransaction,
  Response,
  Transaction,
}
import java.util.UUID
import scala.concurrent.duration._
import org.apache.pekko.actor.testkit.typed.scaladsl.FishingOutcomes

class TransactorSuite extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  "Transactor" must {
    "When empty return empty" in {
      val transactor = testKit.spawn(Transactor[String](50.millis))
      val inbox = testKit.createTestProbe[Response[String]]()

      transactor ! Dequeue(inbox.ref)
      inbox.expectMessage(Empty())
    }
  }
  "Return the payload enqueued" in {
    val transactor = testKit.spawn(Transactor[String](50.millis))
    val inbox = testKit.createTestProbe[Response[String]]()

    val jsonPayload = "{\"awesome\": \"yes\"}"
    transactor ! Enqueue("+18009999999", jsonPayload)

    transactor ! Dequeue(inbox.ref)
    val response = inbox.receiveMessage()
    transactor ! CompleteTransaction[String](UUID.fromString(response.asInstanceOf[Transaction[String]].id))

    response shouldBe a [Transaction[String]]
    response.asInstanceOf[Transaction[String]].jsonObject should equal (jsonPayload)

    transactor ! Dequeue(inbox.ref)
    val emptyResponse = inbox.receiveMessage()
    emptyResponse shouldBe a [Empty[String]]
  }
  "Should enqueue the failed transaction" in {

    val transactor = testKit.spawn(Transactor[String]())
    val inbox = testKit.createTestProbe[Response[String]]()

    val jsonPayload = "{\"awesome\": \"yes\"}"
    transactor ! Enqueue("+18009999999", jsonPayload)

    transactor ! Dequeue(inbox.ref)
    val transaction = inbox.expectMessageType[Transaction[String]]
    transaction.jsonObject should equal (jsonPayload)
    transactor ! FailedTransaction(UUID.fromString(transaction.id), transaction.identifier, transaction.jsonObject)

    transactor ! Dequeue(inbox.ref)

    inbox.fishForMessage(50.millis)(m => {
      m match {
        case Empty() => {
          transactor ! Dequeue(inbox.ref)
          FishingOutcomes.continueAndIgnore
        }
        case _: Transaction[String] => FishingOutcomes.complete
      }
    })
  }

  "Should enqueue the timed out transaction" in {

    val transactor = testKit.spawn(Transactor[String](50.millis))
    val inbox = testKit.createTestProbe[Response[String]]()

    val jsonPayload = "{\"awesome\": \"yes\"}"
    transactor ! Enqueue("+18009999999", jsonPayload)

    transactor ! Dequeue(inbox.ref)
    val transaction = inbox.expectMessageType[Transaction[String]]
    transaction.jsonObject should equal (jsonPayload)

    transactor ! Dequeue(inbox.ref)

    // loop until we get the message
    val responses = inbox.fishForMessage(500.millis)(m => {
      m match {
        case Empty() => {
          transactor ! Dequeue(inbox.ref)
          FishingOutcomes.continueAndIgnore
        }
        case _: Transaction[String] => FishingOutcomes.complete
      }
    })

    transactor ! CompleteTransaction(UUID.fromString(responses.head.asInstanceOf[Transaction[String]].id))
  }
}
