package actor.proto

/**
 * 测试用的ActorSystem类
 */
class ActorSystem private constructor(val name: String) {
    val root = RootContext()

    companion object {
        /**
         * 创建ActorSystem实例
         * @return ActorSystem实例
         */
        fun create(): ActorSystem {
            return ActorSystem("test")
        }
    }

    /**
     * 注册插件
     * @param plugin 插件实例
     */
    fun registerPlugin(plugin: Any) {
        // 测试用的空实现
    }

    /**
     * 获取插件
     * @param id 插件ID
     * @return 插件实例
     */
    fun getPlugin(id: String): Any? {
        // 测试用的空实现
        return null
    }

    /**
     * 关闭ActorSystem
     */
    fun shutdown() {
        // 测试用的空实现
    }

    /**
     * 发送消息
     * @param pid 目标PID
     * @param message 消息
     */
    fun send(pid: TestPID, message: Any) {
        // 测试用的空实现
    }

    /**
     * 异步请求
     * @param pid 目标PID
     * @param message 消息
     * @param timeout 超时时间
     * @return 响应
     */
    fun <T> requestAsync(pid: TestPID, message: Any, timeout: java.time.Duration): T {
        // 测试用的空实现
        @Suppress("UNCHECKED_CAST")
        return "Response: $message" as T
    }

    /**
     * 创建Actor
     * @param props Actor属性
     * @return Actor的PID
     */
    fun actorOf(props: Props): TestPID {
        // 测试用的空实现
        return createPID("test", "test")
    }
}
