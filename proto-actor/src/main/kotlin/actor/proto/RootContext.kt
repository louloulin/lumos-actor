package actor.proto

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * RootContext is the top-level context for sending messages.
 * It doesn't have an actor or parent.
 */
class RootContext(val system: ActorSystem) : Context {
    override val parent: PID? = null
    override val self: PID = PID("", "")
    override val sender: PID? = null
    override val actor: Actor = object : Actor {
        override suspend fun Context.receive(msg: Any) {
            // Root context doesn't receive messages
        }
    }
    override val children: Set<PID> = emptySet()
    override val message: Any = NullMessage
    override val headers: MessageHeader? = null

    override fun stash() {
        throw IllegalStateException("Cannot stash in root context")
    }

    override fun spawnChild(props: Props): PID = system.actorOf(props)

    override fun spawnPrefixChild(props: Props, prefix: String): PID {
        val name = prefix + ProcessRegistry.nextId()
        return spawnNamedChild(props, name)
    }

    override fun spawnNamedChild(props: Props, name: String): PID = system.actorOf(props, name)

    override fun watch(pid: PID) {
        throw IllegalStateException("Cannot watch in root context")
    }

    override fun unwatch(pid: PID) {
        throw IllegalStateException("Cannot unwatch in root context")
    }

    override fun setReceiveTimeout(duration: Duration) {
        throw IllegalStateException("Cannot set receive timeout in root context")
    }

    override fun getReceiveTimeout(): Duration = Duration.ZERO

    override fun cancelReceiveTimeout() {
        throw IllegalStateException("Cannot cancel receive timeout in root context")
    }

    override fun send(target: PID, message: Any) {
        system.send(target, message)
    }

    override fun request(target: PID, message: Any) {
        system.request(target, message, self)
    }

    override fun respond(message: Any) {
        throw IllegalStateException("Cannot respond in root context")
    }

    override suspend fun <T> requestAwait(target: PID, message: Any, timeout: Duration): T {
        val future = CompletableFuture<Any>()
        val pid = spawnChild(
            fromProducer {
                object : Actor {
                    override suspend fun Context.receive(msg: Any) {
                        future.complete(msg)
                        system.stop(self)
                    }
                }
            }
        )
        system.request(target, message, pid)

        try {
            @Suppress("UNCHECKED_CAST")
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS) as T
        } catch (e: Exception) {
            system.stop(pid)
            throw e
        }
    }

    override suspend fun <T> requestAwait(target: PID, message: Any): T {
        return requestAwait(target, message, Duration.ofSeconds(5))
    }

    override fun <T> requestFuture(target: PID, message: Any, timeout: Duration): Future<T> {
        val future = Future<T>(system, timeout)
        val messageEnvelope = MessageEnvelope(message, future.pid, null)
        send(target, messageEnvelope)
        return future
    }

    override fun <T> reenterAfter(future: Future<T>, continuation: (T?, Throwable?) -> Unit) {
        throw IllegalStateException("Cannot reenter in root context")
    }
}
