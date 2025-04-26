package actor.proto

typealias PID = Protos.PID

fun PID(address: String, id: String): PID {
    val p = PID.newBuilder()
    p.address = address
    p.id = id
    return p.build()
}

fun PID.isLocal(): Boolean = address == ProcessRegistry.noHost || address == ProcessRegistry.address
fun PID.isLocal(registry: ProcessRegistryImpl): Boolean = address == registry.address || address == ProcessRegistryImpl.noHost

internal fun PID.cachedProcess(): Process? {
    if (cachedProcess_ == null) {
        cachedProcess_ = ActorSystem.default().processRegistry().get(this)
    }
    return cachedProcess_
}

internal fun PID.cachedProcess(registry: ProcessRegistryImpl): Process? {
    if (cachedProcess_ == null) {
        cachedProcess_ = registry.get(this)
    }
    return cachedProcess_
}

fun PID.toShortString(): String {
    return "$address/$id"
}
