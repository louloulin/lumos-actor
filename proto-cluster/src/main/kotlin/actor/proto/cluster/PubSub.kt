package actor.proto.cluster

import actor.proto.Actor
import actor.proto.Context
import actor.proto.PID
import actor.proto.Props
import actor.proto.fromProducer
import actor.proto.cluster.pubsub.BatchPublishRequest
import actor.proto.cluster.pubsub.BatchingProducer
import actor.proto.cluster.pubsub.BatchingProducerConfig
import actor.proto.cluster.pubsub.PubSubBatch
import actor.proto.cluster.pubsub.PubSubBatchResponse
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

private val logger = KotlinLogging.logger {}

/**
 * PubSub provides publish-subscribe functionality for the cluster.
 */
class PubSub(private val cluster: Cluster) {
    private val topics = ConcurrentHashMap<String, Topic>()
    private lateinit var _pid: PID

    /**
     * Get the PID of the PubSub actor.
     * @return The PID of the PubSub actor.
     */
    val pid: PID
        get() = _pid

    init {
        // Start the pubsub actor
        val props = fromProducer { PubSubActor(this) }
        _pid = cluster.actorSystem.actorOf(props)
    }

    /**
     * Subscribe to a topic.
     * @param topic The topic to subscribe to.
     * @param subscriber The PID of the subscriber.
     */
    fun subscribe(topic: String, subscriber: PID) {
        val topicObj = topics.computeIfAbsent(topic) { Topic(it) }
        topicObj.addSubscriber(subscriber)
    }

    /**
     * Unsubscribe from a topic.
     * @param topic The topic to unsubscribe from.
     * @param subscriber The PID of the subscriber.
     */
    fun unsubscribe(topic: String, subscriber: PID) {
        val topicObj = topics[topic] ?: return
        topicObj.removeSubscriber(subscriber)

        // Remove the topic if it has no subscribers
        if (topicObj.subscriberCount() == 0) {
            topics.remove(topic)
        }
    }

    /**
     * Publish a message to a topic.
     * @param topic The topic to publish to.
     * @param message The message to publish.
     */
    fun publish(topic: String, message: Any) {
        val topicObj = topics[topic] ?: return
        topicObj.publish(message)
    }

    /**
     * Publish a batch of messages to a topic.
     * @param batch The batch to publish.
     * @return The response indicating success or failure.
     */
    fun publishBatch(batch: PubSubBatch): PubSubBatchResponse {
        val topicObj = topics[batch.topic] ?: return PubSubBatchResponse(false, "Topic not found: ${batch.topic}")

        try {
            topicObj.publishBatch(batch.messages)
            return PubSubBatchResponse(true)
        } catch (e: Exception) {
            logger.error(e) { "Error publishing batch to topic ${batch.topic}" }
            return PubSubBatchResponse(false, e.message)
        }
    }

    /**
     * Create a batching producer for a topic.
     * @param topic The topic to publish to.
     * @param config The configuration for the batching producer.
     * @return The batching producer.
     */
    fun batchingProducer(topic: String, config: BatchingProducerConfig = BatchingProducerConfig()): BatchingProducer {
        return BatchingProducer(cluster, topic, config)
    }

    /**
     * Get a topic by name.
     * @param topic The name of the topic.
     * @return The topic, or null if not found.
     */
    fun getTopic(topic: String): Topic? {
        return topics[topic]
    }

    /**
     * Get all topics.
     * @return A map of topic names to topics.
     */
    fun getAllTopics(): Map<String, Topic> {
        return topics.toMap()
    }
}

/**
 * Topic represents a pubsub topic.
 */
class Topic(val name: String) {
    private val subscribers = ConcurrentHashMap.newKeySet<PID>()

    /**
     * Add a subscriber to the topic.
     * @param subscriber The PID of the subscriber.
     */
    fun addSubscriber(subscriber: PID) {
        subscribers.add(subscriber)
    }

    /**
     * Remove a subscriber from the topic.
     * @param subscriber The PID of the subscriber.
     */
    fun removeSubscriber(subscriber: PID) {
        subscribers.remove(subscriber)
    }

    /**
     * Get the number of subscribers.
     * @return The number of subscribers.
     */
    fun subscriberCount(): Int {
        return subscribers.size
    }

    /**
     * Publish a message to all subscribers.
     * @param message The message to publish.
     */
    fun publish(message: Any) {
        for (subscriber in subscribers) {
            try {
                // TODO: Implement remote message sending
                // cluster.actorSystem.send(subscriber, message)
            } catch (e: Exception) {
                logger.error(e) { "Error publishing message to subscriber $subscriber" }
            }
        }
    }

    /**
     * Publish a batch of messages to all subscribers.
     * @param messages The messages to publish.
     */
    fun publishBatch(messages: List<Any>) {
        for (message in messages) {
            publish(message)
        }
    }

    /**
     * Get all subscribers.
     * @return A set of subscriber PIDs.
     */
    fun getSubscribers(): Set<PID> {
        return subscribers.toSet()
    }
}

/**
 * PubSubActor handles pubsub messages.
 */
class PubSubActor(private val pubSub: PubSub) : Actor {
    override suspend fun Context.receive(msg: Any) {
        when (msg) {
            is SubscriptionRequest -> {
                pubSub.subscribe(msg.topic, msg.subscriber)
                sender?.let { send(it, SubscriptionResponse(true)) }
            }
            is UnsubscriptionRequest -> {
                pubSub.unsubscribe(msg.topic, msg.subscriber)
                sender?.let { send(it, UnsubscriptionResponse(true)) }
            }
            is PubSubMessage -> {
                pubSub.publish(msg.topic, msg.message)
            }
            is BatchPublishRequest -> {
                val response = pubSub.publishBatch(msg.batch)
                msg.sender?.let { send(it, response) }
            }
        }
    }
}

/**
 * SubscriptionRequest represents a request to subscribe to a topic.
 */
data class SubscriptionRequest(
    val topic: String,
    val subscriber: PID
)

/**
 * SubscriptionResponse represents a response to a subscription request.
 */
data class SubscriptionResponse(
    val success: Boolean,
    val errorMessage: String? = null
)

/**
 * UnsubscriptionRequest represents a request to unsubscribe from a topic.
 */
data class UnsubscriptionRequest(
    val topic: String,
    val subscriber: PID
)

/**
 * UnsubscriptionResponse represents a response to an unsubscription request.
 */
data class UnsubscriptionResponse(
    val success: Boolean,
    val errorMessage: String? = null
)

/**
 * PubSubMessage represents a message published to a topic.
 */
data class PubSubMessage(
    val topic: String,
    val message: Any
)
