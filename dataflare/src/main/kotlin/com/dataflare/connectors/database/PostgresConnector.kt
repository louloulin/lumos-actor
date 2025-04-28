package com.dataflare.connectors.database

import com.dataflare.connectors.*
import com.dataflare.core.Message
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * PostgreSQL 连接器配置
 */
@Serializable
data class PostgresConfig(
    override val type: String = "postgres",
    val connectionString: String,
    val table: String,
    val columns: List<String> = emptyList(),
    val query: String = "",
    val batchSize: Int = 100
) : Config()

/**
 * PostgreSQL 输入连接器
 */
class PostgresInputConnector : Input {
    private lateinit var config: PostgresConfig
    private var connection: Connection? = null
    private var isConnected = false
    private var currentOffset = 0

    override suspend fun configure(config: Config) {
        if (config !is PostgresConfig) {
            throw IllegalArgumentException("Expected PostgresConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to PostgreSQL: ${maskConnectionString(config.connectionString)}" }

            // 加载 PostgreSQL JDBC 驱动
            Class.forName("org.postgresql.Driver")

            // 创建连接
            connection = DriverManager.getConnection(config.connectionString)
            isConnected = true

            logger.info { "Connected to PostgreSQL successfully" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to PostgreSQL: ${e.message}" }
            false
        }
    }

    override suspend fun read(ctx: Context): Message? = withContext(Dispatchers.IO) {
        if (!isConnected || connection == null) {
            throw IllegalStateException("Not connected to PostgreSQL")
        }

        try {
            val query = if (config.query.isNotBlank()) {
                config.query
            } else {
                val columnsStr = if (config.columns.isEmpty()) "*" else config.columns.joinToString(", ")
                "SELECT $columnsStr FROM ${config.table} LIMIT ${config.batchSize} OFFSET $currentOffset"
            }

            connection?.createStatement()?.use { statement ->
                statement.executeQuery(query).use { resultSet ->
                    if (resultSet.next()) {
                        currentOffset++
                        resultSetToMessage(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Error reading from PostgreSQL: ${e.message}" }
            null
        }
    }

    override suspend fun close(ctx: Context): Unit = withContext(Dispatchers.IO) {
        logger.info { "Closing PostgreSQL input connector" }
        try {
            connection?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing PostgreSQL connection: ${e.message}" }
        } finally {
            connection = null
            isConnected = false
        }
    }

    private fun resultSetToMessage(resultSet: ResultSet): Message {
        val metadata = resultSet.metaData
        val columnCount = metadata.columnCount
        val data = mutableMapOf<String, Any?>()

        for (i in 1..columnCount) {
            val columnName = metadata.getColumnName(i)
            val value = resultSet.getObject(i)
            data[columnName] = value
        }

        return Message.create(data)
    }

    private fun maskConnectionString(connectionString: String): String {
        // 简单的掩码处理，隐藏密码
        return connectionString.replace(Regex("password=([^&]*)"), "password=****")
    }
}

/**
 * PostgreSQL 输出连接器
 */
class PostgresOutputConnector : Output {
    private lateinit var config: PostgresConfig
    private var connection: Connection? = null
    private var isConnected = false

    override suspend fun configure(config: Config) {
        if (config !is PostgresConfig) {
            throw IllegalArgumentException("Expected PostgresConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to PostgreSQL: ${maskConnectionString(config.connectionString)}" }

            // 加载 PostgreSQL JDBC 驱动
            Class.forName("org.postgresql.Driver")

            // 创建连接
            connection = DriverManager.getConnection(config.connectionString)
            isConnected = true

            logger.info { "Connected to PostgreSQL successfully" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to PostgreSQL: ${e.message}" }
            false
        }
    }

    override suspend fun write(ctx: Context, batch: List<Message>): WriteResult = withContext(Dispatchers.IO) {
        if (!isConnected || connection == null) {
            throw IllegalStateException("Not connected to PostgreSQL")
        }

        var recordsWritten = 0
        val errors = mutableListOf<String>()

        try {
            connection?.autoCommit = false

            batch.forEach { message ->
                try {
                    val insertSql = createInsertStatement(message)
                    connection?.createStatement()?.use { statement ->
                        val rowsAffected = statement.executeUpdate(insertSql)
                        if (rowsAffected > 0) {
                            recordsWritten++
                        }
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Error writing message to PostgreSQL: ${e.message}" }
                    errors.add("Error writing message: ${e.message}")
                }
            }

            connection?.commit()

            WriteResult(
                success = errors.isEmpty(),
                recordsWritten = recordsWritten,
                errors = errors
            )
        } catch (e: Exception) {
            try {
                connection?.rollback()
            } catch (rollbackEx: Exception) {
                logger.error(rollbackEx) { "Error rolling back transaction: ${rollbackEx.message}" }
            }

            logger.error(e) { "Error writing batch to PostgreSQL: ${e.message}" }

            WriteResult(
                success = false,
                recordsWritten = recordsWritten,
                errors = listOf("Error writing batch: ${e.message}")
            )
        } finally {
            try {
                connection?.autoCommit = true
            } catch (e: Exception) {
                logger.error(e) { "Error resetting autoCommit: ${e.message}" }
            }
        }
    }

    override suspend fun close(ctx: Context): Unit = withContext(Dispatchers.IO) {
        logger.info { "Closing PostgreSQL output connector" }
        try {
            connection?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing PostgreSQL connection: ${e.message}" }
        } finally {
            connection = null
            isConnected = false
        }
    }

    private fun createInsertStatement(message: Message): String {
        val columns = if (config.columns.isEmpty()) {
            message.payload.keys
        } else {
            config.columns
        }

        val columnList = columns.joinToString(", ")
        val valuePlaceholders = columns.joinToString(", ") { "?" }

        val sql = "INSERT INTO ${config.table} ($columnList) VALUES ($valuePlaceholders)"

        // 替换占位符为实际值
        return columns.fold(sql) { acc, column ->
            val value = message.payload[column]
            val formattedValue = formatValue(value)
            acc.replaceFirst("?", formattedValue)
        }
    }

    private fun formatValue(value: Any?): String {
        return when (value) {
            null -> "NULL"
            is String -> "'${value.replace("'", "''")}'"
            is Number -> value.toString()
            is Boolean -> if (value) "TRUE" else "FALSE"
            is Date -> "'${value}'"
            else -> "'${value.toString().replace("'", "''")}'"
        }
    }

    private fun maskConnectionString(connectionString: String): String {
        // 简单的掩码处理，隐藏密码
        return connectionString.replace(Regex("password=([^&]*)"), "password=****")
    }
}

/**
 * PostgreSQL 连接器工厂
 */
class PostgresConnectorFactory : ConnectorFactory {
    override fun createInput(config: Config): Input {
        return PostgresInputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }

    override fun createOutput(config: Config): Output {
        return PostgresOutputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }
}
