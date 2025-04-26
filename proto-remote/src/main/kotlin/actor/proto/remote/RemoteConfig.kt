package actor.proto.remote

data class RemoteConfig(
        val hostname: String,
        val port: Int,
        val endpointWriterBatchSize: Int = 1000,
        val advertisedHostname: String? = null,
        val advertisedPort: Int? = null,
        val idleTimeout: Long? = null,
        val keepAliveTime: Long? = null,
        val keepAliveTimeout: Long? = null,
        val keepAliveWithoutCalls : Boolean? = null,
        val usePlainText : Boolean = true
) {
    companion object {
        fun create(hostname: String, port: Int): RemoteConfig {
            return RemoteConfig(hostname, port)
        }
    }
}
