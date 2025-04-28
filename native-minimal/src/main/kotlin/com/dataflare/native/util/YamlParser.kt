package com.dataflare.native.util

import com.dataflare.native.model.WorkflowConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

/**
 * YAML 解析工具类
 */
object YamlParser {
    private val mapper = ObjectMapper(YAMLFactory()).apply {
        registerModule(KotlinModule.Builder().build())
    }
    
    /**
     * 解析工作流配置文件
     *
     * @param file 配置文件
     * @return 工作流配置对象
     */
    fun parseWorkflowConfig(file: File): WorkflowConfig {
        return mapper.readValue(file)
    }
    
    /**
     * 解析工作流配置文件
     *
     * @param content 配置文件内容
     * @return 工作流配置对象
     */
    fun parseWorkflowConfig(content: String): WorkflowConfig {
        return mapper.readValue(content)
    }
}
