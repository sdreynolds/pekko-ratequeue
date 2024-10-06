## Pekko RateQueue Implementation

This is a small proof of concept written after leaving Twilio. At Twilio, there is a key service called RateQueue that sits in between the Twilio REST API and Carrier APIs -- specifically for Text Messages. This service was first [presented at RedisConf in 2012](https://vimeo.com/52569901) and described in a [Chaos Engineering](https://www.twilio.com/en-us/blog/chaos-engineering-ratequeue-ha-html) blog post. The RateQueue service ensures that all text messages are sent to carriers at a rate of *1 message per second per phone number* and that rate is kept independent of the number of size of Twilio REST API requests. This prevents both:

1. a single phone number from spamming before anti-spam and anti-phishing algorithms can detect and block the message
2. denial of service attack against downstream systems and carrier connections

This problem is one that is used during Twilio interviews because it is a problem easy to grasp, difficult to solve and unique to Twilio. During my time at Twilio I found this problem to be interesting and Twilio's solution to the problem has long stood the test of time. This is not an implementation of Twilio's solution but is instead an implementation using [Apache Pekko](https://pekko.apache.org/). The core idea behind this proof of concept is to explore Event Driven Design and Pekko / Akka framework for implementing Event Driven systems.

### What it is
This is just a proof of concept, it doesn't run a HTTP service and it doesn't have a executable jar for someone to use. It just has a few implementations and some tests.

The implementation starts with the `Transactor` which is responsible for managing the lifetime of a `Transaction`. A `Transaction` is a promise to the Consumer that the message it has given the Consumer will not be given to another Consumer until the lease on the `Transaction` expires. The Consumer promises to commit the `Transaction` back to the `Transactor` so that the `Transactor` can delete the message from the system. In the event the Consumer does not commit back, the message will be placed back into the Queue and be handed off to another Consumer.

The `Transactor` queries a `QueueOfQueues` object which handles enqueuing and dequeuing messages. `QueueOfQueues` handles these two requests by managing it's own set of `PhoneQueue` which hold each message for a given outbound `PhoneNumber`. When a new message arrives at an empty `QueueOfQueues` it creates a `PhoneQueue` and adds the new message to it. The `QueueOfQueues` keeps a `PriorityQueue` of phone numbers that is updated when a message is dequeued from the phone number. When dequeue event returns a message for the consumer, the `QueueOfQueues` adds the phone number back into the `PriorityQueue` with score adjusted to ensure it doesn't get used right away.


### Usage
This is an SBT powered project and therefore, you can execute `sbt test` and have the whole project built and all the tests run.
