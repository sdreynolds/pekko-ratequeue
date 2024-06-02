package com.github.sdreynolds.ratequeue

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object Arena {
  trait Command
  case class Enqueue(phoneNumber: String, jsonObject: String) extends Command

  def apply(): Behavior[Command] =
    Behaviors.receiveMessage(cmd => {
      cmd match {
        case Enqueue(number, jsonObject) => {
          println(jsonObject)
          Behaviors.same
        }
      }
    })
}

object RatequeueApp extends App {
  val testSystem = ActorSystem(Arena(), "RateQueue")
  testSystem ! Arena.Enqueue("345", "sup")
}
