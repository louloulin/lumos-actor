package actor.proto.cluster.libp2p

import io.libp2p.core.Stream
import io.libp2p.core.multistream.ProtocolBinding
import io.libp2p.core.multistream.ProtocolId
import io.libp2p.protocol.ProtocolHandler
import io.libp2p.protocol.ProtocolMessageHandler
import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture

/**
 * P2PClusterProtocol 是集群通信协议
 */
class P2PClusterProtocol(
    private val provider: P2PClusterProvider
) : ProtocolBinding<P2PClusterProtocol.Controller> {
    
    companion object {
        const val PROTOCOL_ID: ProtocolId = "/protoactor/cluster/1.0.0"
    }
    
    override fun getProtocolId(): ProtocolId = PROTOCOL_ID
    
    override fun initChannel(stream: Stream, isInitiator: Boolean): CompletableFuture<Controller> {
        val handler = Handler()
        stream.pushHandler(handler)
        
        return CompletableFuture.completedFuture(handler)
    }
    
    /**
     * 协议控制器接口
     */
    interface Controller {
        /**
         * 发送消息
         */
        fun sendMessage(message: ByteArray)
    }
    
    /**
     * 协议处理器
     */
    inner class Handler : ProtocolMessageHandler<ByteBuf>, Controller {
        private lateinit var stream: Stream
        
        override fun onActivated(stream: Stream) {
            this.stream = stream
        }
        
        override fun onMessage(stream: Stream, msg: ByteBuf) {
            try {
                val bytes = ByteArray(msg.readableBytes())
                msg.readBytes(bytes)
                
                // 将消息转发给 gossiper 处理
                provider.getGossiper().handleRawMessage(bytes, stream.remotePeerId())
            } catch (e: Exception) {
                // 记录错误但不关闭流
                println("Error handling message: ${e.message}")
            }
        }
        
        override fun sendMessage(message: ByteArray) {
            val buffer = stream.nettyChannel().alloc().buffer(message.size)
            buffer.writeBytes(message)
            stream.writeAndFlush(buffer)
        }
    }
}
