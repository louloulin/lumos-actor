package actor.proto

interface SenderContext {
    val message: Any?
    val headers: MessageHeader?
    val sender: PID?
    val self: PID

    fun send(target: PID, message: Any)
    fun request(target: PID, message: Any)
}

