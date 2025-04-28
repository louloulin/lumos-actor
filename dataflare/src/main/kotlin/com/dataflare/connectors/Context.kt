package com.dataflare.connectors

/**
 * 上下文接口 - 提供处理过程中的上下文信息
 */
interface Context {
    /**
     * 上下文属性
     */
    val properties: MutableMap<String, Any>
}
