package actor.proto.cluster

import actor.proto.Actor
import actor.proto.Context
import actor.proto.PID
import actor.proto.Props
import actor.proto.fromProducer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

private val logger = KotlinLogging.logger {}

/**
 * Gossiper is responsible for gossiping state between cluster members.
 */
class Gossiper(private val cluster: Cluster) {
    private val state = ConcurrentHashMap<String, GossipState>()
    private val versions = ConcurrentHashMap<String, AtomicLong>()
    private val consensusChecks = ConcurrentHashMap<String, ConsensusCheck>()
    private lateinit var pid: PID

    init {
        // Start the gossip actor
        val props = fromProducer { GossipActor(this) }
        pid = cluster.actorSystem.actorOf(props)

        // Start the gossip loop
        cluster.actorSystem.actorOf(fromProducer { GossipLoopActor(this) })
    }

    /**
     * Register a consensus check for a key.
     * @param key The key to check consensus for.
     * @param check The check function.
     * @return The consensus check.
     */
    fun registerConsensusCheck(key: String, check: (Any) -> Any?): ConsensusCheck {
        val consensusCheck = ConsensusCheck(check)
        consensusChecks[key] = consensusCheck
        return consensusCheck
    }

    /**
     * Update the gossip state for a key.
     * @param key The key to update.
     * @param value The new value.
     */
    fun updateState(key: String, value: Any) {
        val version = versions.computeIfAbsent(key) { AtomicLong(0) }.incrementAndGet()
        val gossipState = GossipState(key, value, version)
        state[key] = gossipState

        // Publish the update
        val eventStream = cluster.actorSystem.eventStream()
        eventStream.publish(GossipUpdate(key, value, version))
    }

    /**
     * Get the gossip state for a key.
     * @param key The key to get.
     * @return The gossip state, or null if not found.
     */
    fun getState(key: String): GossipState? {
        return state[key]
    }

    /**
     * Get all gossip states.
     * @return A map of keys to gossip states.
     */
    fun getAllStates(): Map<String, GossipState> {
        return state.toMap()
    }

    /**
     * Handle a gossip request.
     * @param request The gossip request.
     * @return The gossip response.
     */
    fun handleGossipRequest(request: GossipRequest): GossipResponse {
        val response = mutableMapOf<String, GossipState>()

        // Check if we have newer versions of any keys
        for ((key, theirState) in request.states) {
            val ourState = state[key]
            if (ourState != null && ourState.version > theirState.version) {
                response[key] = ourState
            }
        }

        // Check if we have keys they don't have
        for ((key, ourState) in state) {
            if (!request.states.containsKey(key)) {
                response[key] = ourState
            }
        }

        return GossipResponse(response)
    }

    /**
     * Handle a gossip response.
     * @param response The gossip response.
     */
    fun handleGossipResponse(response: GossipResponse) {
        for ((key, theirState) in response.states) {
            val ourState = state[key]
            if (ourState == null || theirState.version > ourState.version) {
                state[key] = theirState
                versions.computeIfAbsent(key) { AtomicLong(0) }.set(theirState.version)

                // Publish the update
                val eventStream = cluster.actorSystem.eventStream()
                eventStream.publish(GossipUpdate(key, theirState.value, theirState.version))

                // Check for consensus
                consensusChecks[key]?.checkConsensus(theirState.value)
            }
        }
    }

    /**
     * Gossip with a random subset of members.
     */
    suspend fun gossip() {
        val members = cluster.memberList.getMembers()
            .filter { it.status == MemberStatus.ALIVE }
            .map { it.id }
            .toMutableList()

        // Remove our own ID
        members.remove(cluster.actorSystem.address)

        if (members.isEmpty()) {
            return
        }

        // Select a random subset of members to gossip with
        val fanOut = minOf(cluster.config.gossipFanOut, members.size)
        val selectedMembers = mutableListOf<String>()

        for (i in 0 until fanOut) {
            val index = Random.nextInt(members.size)
            selectedMembers.add(members.removeAt(index))
        }

        // Gossip with selected members
        for (memberId in selectedMembers) {
            try {
                // Create a gossip request
                val request = GossipRequest(state.toMap())

                // Send the request to the member
                // TODO: Implement remote gossip request

            } catch (e: Exception) {
                logger.error(e) { "Error gossiping with member $memberId" }
            }
        }
    }
}

/**
 * GossipState represents the state of a key in the gossip protocol.
 */
data class GossipState(
    val key: String,
    val value: Any,
    val version: Long
)

/**
 * GossipUpdate represents an update to the gossip state.
 */
data class GossipUpdate(
    val key: String,
    val value: Any,
    val version: Long
)

/**
 * GossipRequest represents a request for gossip state.
 */
data class GossipRequest(
    val states: Map<String, GossipState>
)

/**
 * GossipResponse represents a response to a gossip request.
 */
data class GossipResponse(
    val states: Map<String, GossipState>
)

/**
 * ConsensusCheck checks for consensus on a key.
 */
class ConsensusCheck(private val check: (Any) -> Any?) {
    private val values = ConcurrentHashMap<Any, Int>()
    private var consensus: Any? = null

    /**
     * Check for consensus on a value.
     * @param value The value to check.
     */
    fun checkConsensus(value: Any) {
        val key = check(value) ?: return
        val count = values.compute(key) { _, v -> (v ?: 0) + 1 } ?: 1

        // TODO: Implement consensus algorithm
        consensus = key
    }

    /**
     * Try to get the consensus value.
     * @return The consensus value and whether consensus has been reached.
     */
    fun tryGetConsensus(): Pair<Any?, Boolean> {
        return consensus to (consensus != null)
    }
}

/**
 * GossipActor handles gossip messages.
 */
class GossipActor(private val gossiper: Gossiper) : Actor {
    override suspend fun Context.receive(msg: Any) {
        when (msg) {
            is GossipRequest -> {
                val response = gossiper.handleGossipRequest(msg)
                sender?.let { send(it, response) }
            }
            is GossipResponse -> {
                gossiper.handleGossipResponse(msg)
            }
        }
    }
}

/**
 * GossipLoopActor periodically gossips with other members.
 */
class GossipLoopActor(private val gossiper: Gossiper) : Actor {
    override suspend fun Context.receive(msg: Any) {
        when (msg) {
            is actor.proto.Started -> {
                // Start the gossip loop
                CoroutineScope(kotlinx.coroutines.Dispatchers.Default).launch {
                    while (true) {
                        try {
                            gossiper.gossip()
                        } catch (e: Exception) {
                            logger.error(e) { "Error in gossip loop" }
                        }

                        delay(1000) // Use a fixed delay for now
                    }
                }
            }
        }
    }
}
