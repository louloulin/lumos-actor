package com.dataflare.native.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class WorkflowConfig(
    val name: String,
    val description: String? = null,
    val source: SourceConfig,
    val processors: List<ProcessorConfig> = emptyList(),
    val sink: SinkConfig
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SourceConfig(
    val type: String,
    val config: Map<String, Any>
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class ProcessorConfig(
    val name: String,
    val type: String,
    val config: Map<String, Any>
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SinkConfig(
    val type: String,
    val config: Map<String, Any>
)
