package actor.proto.cluster.libp2p.interop

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * StandardMessage 是一个标准化的消息格式，用于跨语言通信
 * 
 * 消息格式为 JSON，包含以下字段：
 * - type: 消息类型 (string)
 * - sender: 发送者 ID (string)
 * - target: 目标 ID (string, 可选)
 * - requestId: 请求 ID (string, 可选)
 * - payload: 消息内容 (base64 编码的字符串)
 * - metadata: 元数据 (object, 可选)
 */
data class StandardMessage(
    val type: MessageType,
    val sender: String,
    val target: String? = null,
    val requestId: String? = null,
    val payload: ByteArray,
    val metadata: Map<String, String> = emptyMap()
) {
    companion object {
        private val gson: Gson = GsonBuilder().create()
        
        /**
         * 从 JSON 字符串解析消息
         */
        fun fromJson(json: String): StandardMessage {
            val jsonObject = gson.fromJson(json, JsonObject::class.java)
            
            val type = MessageType.valueOf(jsonObject.get("type").asString)
            val sender = jsonObject.get("sender").asString
            val target = if (jsonObject.has("target")) jsonObject.get("target").asString else null
            val requestId = if (jsonObject.has("requestId")) jsonObject.get("requestId").asString else null
            
            // 解码 payload
            val payloadBase64 = jsonObject.get("payload").asString
            val payload = Base64.getDecoder().decode(payloadBase64)
            
            // 解析元数据
            val metadata = mutableMapOf<String, String>()
            if (jsonObject.has("metadata")) {
                val metadataObj = jsonObject.getAsJsonObject("metadata")
                metadataObj.entrySet().forEach { entry ->
                    metadata[entry.key] = entry.value.asString
                }
            }
            
            return StandardMessage(type, sender, target, requestId, payload, metadata)
        }
        
        /**
         * 从字节数组解析消息
         */
        fun fromBytes(bytes: ByteArray): StandardMessage {
            val json = String(bytes, StandardCharsets.UTF_8)
            return fromJson(json)
        }
    }
    
    /**
     * 转换为 JSON 字符串
     */
    fun toJson(): String {
        val jsonObject = JsonObject()
        
        jsonObject.addProperty("type", type.name)
        jsonObject.addProperty("sender", sender)
        target?.let { jsonObject.addProperty("target", it) }
        requestId?.let { jsonObject.addProperty("requestId", it) }
        
        // 编码 payload
        val payloadBase64 = Base64.getEncoder().encodeToString(payload)
        jsonObject.addProperty("payload", payloadBase64)
        
        // 添加元数据
        if (metadata.isNotEmpty()) {
            val metadataObj = JsonObject()
            metadata.forEach { (key, value) ->
                metadataObj.addProperty(key, value)
            }
            jsonObject.add("metadata", metadataObj)
        }
        
        return gson.toJson(jsonObject)
    }
    
    /**
     * 转换为字节数组
     */
    fun toBytes(): ByteArray {
        return toJson().toByteArray(StandardCharsets.UTF_8)
    }
    
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        
        other as StandardMessage
        
        if (type != other.type) return false
        if (sender != other.sender) return false
        if (target != other.target) return false
        if (requestId != other.requestId) return false
        if (!payload.contentEquals(other.payload)) return false
        if (metadata != other.metadata) return false
        
        return true
    }
    
    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + sender.hashCode()
        result = 31 * result + (target?.hashCode() ?: 0)
        result = 31 * result + (requestId?.hashCode() ?: 0)
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + metadata.hashCode()
        return result
    }
}

/**
 * 消息类型
 */
enum class MessageType {
    ACTOR_MESSAGE,    // Actor 消息
    REQUEST,          // 请求
    RESPONSE,         // 响应
    HEARTBEAT,        // 心跳
    MEMBER_UP,        // 成员上线
    MEMBER_DOWN,      // 成员下线
    MEMBER_LEAVING,   // 成员离开
    PING,             // Ping
    PONG,             // Pong
    ACTOR_ACTIVATION, // Actor 激活
    DHT_PUT,          // DHT 存储
    DHT_GET,          // DHT 获取
    DHT_REMOVE        // DHT 删除
}
