package actor.proto

import actor.proto.diagnostics.Diagnostics
import actor.proto.diagnostics.MatchType
import actor.proto.diagnostics.ProcessInfo
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.future.await

/**
 * ActorSystem is the central management unit for actors.
 * It provides methods for creating, finding, and managing actors.
 */
class ActorSystem(val name: String) {
    private val processRegistryImpl = ProcessRegistryImpl(this)

    val address: String
        get() = processRegistryImpl.address

    fun eventStream(): EventStreamImpl = EventStream
    private val rootContext = RootContext(this)
    private val diagnostics = Diagnostics(this)
    private val hostResolvers = mutableListOf<(PID) -> Process?>()
    val scheduler = Scheduler()
    val deadLetter: Process = DeadLetterProcess

    init {
        processRegistryImpl.registerHostResolver { pid ->
            hostResolvers.forEach { resolver ->
                val process = resolver(pid)
                if (process != null) {
                    return@registerHostResolver process
                }
            }
            deadLetter
        }
    }

    /**
     * Get the root context
     * @return The root context
     */
    fun root(): RootContext = rootContext

    /**
     * Get the PID of the dead letter actor
     * @return The PID of the dead letter actor
     */
    fun deadLetter(): PID = PID(processRegistryImpl.address, "deadletter")

    /**
     * Create a new actor with the given props and a generated ID
     * @param props The props to create the actor with
     * @return The PID of the new actor
     */
    fun actorOf(props: Props): PID {
        val name = processRegistryImpl.nextId()
        return actorOf(props, name)
    }

    /**
     * Create a new actor with the given props and name
     * @param props The props to create the actor with
     * @param name The name of the actor
     * @return The PID of the new actor
     */
    fun actorOf(props: Props, name: String): PID {
        val mailbox = props.mailboxProducer()
        val dispatcher = props.dispatcher
        val process = LocalProcess(mailbox)
        val (self, success) = processRegistryImpl.put(name, process)
        if (!success) {
            throw ProcessNameExistException(name)
        }
        val ctx = ActorContext(props.producer!!, self, props.supervisorStrategy, props.receiveMiddleware, props.senderMiddleware, null)
        mailbox.registerHandlers(ctx, dispatcher)
        mailbox.postSystemMessage(Started)
        mailbox.start()
        return self
    }

    /**
     * Stop the actor with the given PID
     * @param pid The PID of the actor to stop
     */
    fun stop(pid: PID) {
        val process = processRegistryImpl.get(pid)
        process.stop(pid)
    }

    /**
     * Send a poison pill to the actor with the given PID
     * @param pid The PID of the actor to poison
     */
    fun poison(pid: PID) {
        send(pid, PoisonPill.getDefaultInstance())
    }

    /**
     * Send a message to the actor with the given PID
     * @param pid The PID of the actor to send the message to
     * @param message The message to send
     */
    fun send(pid: PID, message: Any) {
        val process = processRegistryImpl.get(pid)
        process.sendUserMessage(pid, message)
    }

    /**
     * Send a message to the actor with the given PID and set the sender
     * @param pid The PID of the actor to send the message to
     * @param message The message to send
     * @param sender The PID of the sender
     */
    fun request(pid: PID, message: Any, sender: PID) {
        val process = processRegistryImpl.get(pid)
        val envelope = MessageEnvelope(message, sender)
        process.sendUserMessage(pid, envelope)
    }

    /**
     * Send a message to the actor with the given PID and wait for a response
     * @param pid The PID of the actor to send the message to
     * @param message The message to send
     * @param timeout The timeout for the request
     * @return The response from the actor
     */
    suspend fun <T> requestAsync(pid: PID, message: Any, timeout: Duration): T {
        return rootContext.requestAwait(pid, message, timeout)
    }

    /**
     * Register a host resolver
     * @param resolver The resolver function
     */
    fun registerHostResolver(resolver: (PID) -> Process?) {
        hostResolvers.add(resolver)
    }

    /**
     * 获取进程注册表实现
     * @return 进程注册表实现
     */
    fun processRegistry(): ProcessRegistryImpl = processRegistryImpl

    /**
     * Get information about a process
     * @param pid The PID of the process
     * @return The process information
     */
    fun getProcessInfo(pid: PID): ProcessInfo {
        return diagnostics.getProcessInfo(pid)
    }

    /**
     * List processes matching a pattern
     * @param pattern The pattern to match
     * @param matchType The type of matching to perform
     * @return The list of PIDs matching the pattern
     */
    fun listProcesses(pattern: String, matchType: MatchType): List<PID> {
        return diagnostics.listProcesses(pattern, matchType)
    }

    /**
     * Get all processes in the system
     * @return A list of all PIDs in the system
     */
    fun getAllProcesses(): List<PID> {
        return diagnostics.getAllProcesses()
    }

    /**
     * Get detailed information about all processes matching a pattern
     * @param pattern The pattern to match
     * @param matchType The type of matching to perform
     * @return A list of ProcessInfo objects for all matching processes
     */
    fun getProcessInfos(pattern: String, matchType: MatchType): List<ProcessInfo> {
        return diagnostics.getProcessInfos(pattern, matchType)
    }

    /**
     * Get detailed information about all processes in the system
     * @return A list of ProcessInfo objects for all processes
     */
    fun getAllProcessInfos(): List<ProcessInfo> {
        return diagnostics.getAllProcessInfos()
    }

    companion object {
        private val systems = ConcurrentHashMap<String, ActorSystem>()
        private val defaultSystemRef = AtomicReference<ActorSystem>(null)

        /**
         * 获取默认的 ActorSystem 实例
         * @return 默认的 ActorSystem 实例
         */
        fun default(): ActorSystem {
            var system = defaultSystemRef.get()
            if (system == null) {
                system = ActorSystem("default")
                if (!defaultSystemRef.compareAndSet(null, system)) {
                    system = defaultSystemRef.get()
                }
            }
            return system
        }

        /**
         * Get or create an ActorSystem with the given name
         * @param name The name of the ActorSystem
         * @return The ActorSystem
         */
        fun get(name: String): ActorSystem {
            return systems.getOrPut(name) { ActorSystem(name) }
        }

        // 已经在上面定义了 default() 方法
    }
}
