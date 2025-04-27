package actor.proto.examples.messagebatch

import actor.proto.*
import actor.proto.mailbox.MessageBatch

/**
 * Example demonstrating the use of MessageBatch.
 *
 * MessageBatch is a message that is sent to the actor and unpacks its payload in the mailbox.
 * This allows you to group messages together and send them as a single message
 * while processing them as individual messages.
 */

// Define a simple message class
data class SimpleMessage(val text: String)

// Define a message batch implementation
class SimpleMessageBatch(private val messages: List<Any>) : MessageBatch {
    override fun getMessages(): List<Any> = messages
}

fun main() {
    // Create an actor system
    ActorSystem("message-batch-example")

    // Create an actor that prints received messages
    val props = fromFunc { msg ->
        when (msg) {
            is SimpleMessage -> {
                println("Received message: ${msg.text}")
            }
            is SimpleMessageBatch -> {
                println("Received batch with ${msg.getMessages().size} messages")
            }
            else -> {
                println("Received unknown message: $msg")
            }
        }
    }

    // Spawn the actor
    val pid = spawn(props)

    // Create a batch of messages
    val messages = List(5) { SimpleMessage("Message $it") }
    val batch = SimpleMessageBatch(messages)

    println("Sending a batch of ${messages.size} messages...")

    // Send the batch to the actor
    send(pid, batch)

    // Wait for user input before exiting
    println("Press enter to exit")
    readLine()

    // Stop the actor
    stop(pid)
}
