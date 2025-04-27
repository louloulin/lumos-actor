package actor.proto

/**
 * 测试用的RootContext类
 */
class RootContext {
    /**
     * 发送消息
     * @param pid 目标PID
     * @param message 消息
     */
    fun send(pid: TestPID, message: Any) {
        // 测试用的空实现
    }

    /**
     * 创建Actor
     * @param props Actor属性
     * @return Actor的PID
     */
    fun spawn(props: Props): TestPID {
        // 测试用的空实现
        return createPID("test", "test")
    }
}
