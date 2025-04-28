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
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * MySQL 连接器配置
 */
@Serializable
data class MySQLConfig(
    override val type: String = "mysql",
    val connectionString: String,
    val table: String,
    val columns: List<String> = emptyList(),
    val query: String = "",
    val batchSize: Int = 100,
    val useSSL: Boolean = false,
    val allowPublicKeyRetrieval: Boolean = false
) : Config()

/**
 * MySQL 输入连接器
 */
class MySQLInputConnector : Input {
    private lateinit var config: MySQLConfig
    private var connection: Connection? = null
    private var isConnected = false
    private var currentOffset = 0

    override suspend fun configure(config: Config) {
        if (config !is MySQLConfig) {
            throw IllegalArgumentException("Expected MySQLConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to MySQL: ${maskConnectionString(config.connectionString)}" }

            // 加载 MySQL JDBC 驱动
            Class.forName("com.mysql.cj.jdbc.Driver")

            // 创建连接
            val connectionStringWithParams = appendConnectionParams(config.connectionString)
            connection = DriverManager.getConnection(connectionStringWithParams)
            isConnected = true

            logger.info { "Connected to MySQL successfully" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to MySQL: ${e.message}" }
            false
        }
    }

    override suspend fun read(ctx: Context): Message? = withContext(Dispatchers.IO) {
        if (!isConnected || connection == null) {
            throw IllegalStateException("Not connected to MySQL")
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
            logger.error(e) { "Error reading from MySQL: ${e.message}" }
            null
        }
    }

    override suspend fun close(ctx: Context): Unit = withContext(Dispatchers.IO) {
        logger.info { "Closing MySQL input connector" }
        try {
            connection?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing MySQL connection: ${e.message}" }
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
        // 掩码处理，隐藏密码
        return connectionString.replace(Regex("password=([^&]*)"), "password=****")
            .replace(Regex("pwd=([^&]*)"), "pwd=****")
    }

    private fun appendConnectionParams(connectionString: String): String {
        val params = mutableListOf<String>()
        
        // 添加 SSL 参数
        params.add("useSSL=${config.useSSL}")
        
        // 添加公钥检索参数
        params.add("allowPublicKeyRetrieval=${config.allowPublicKeyRetrieval}")
        
        // 检查连接字符串是否已包含参数
        return if (connectionString.contains("?")) {
            "$connectionString&${params.joinToString("&")}"
        } else {
            "$connectionString?${params.joinToString("&")}"
        }
    }
}

/**
 * MySQL 输出连接器
 */
class MySQLOutputConnector : Output {
    private lateinit var config: MySQLConfig
    private var connection: Connection? = null
    private var isConnected = false

    override suspend fun configure(config: Config) {
        if (config !is MySQLConfig) {
            throw IllegalArgumentException("Expected MySQLConfig, got ${config::class.simpleName}")
        }
        this.config = config
    }

    override suspend fun connect(ctx: Context): Boolean = withContext(Dispatchers.IO) {
        try {
            logger.info { "Connecting to MySQL: ${maskConnectionString(config.connectionString)}" }

            // 加载 MySQL JDBC 驱动
            Class.forName("com.mysql.cj.jdbc.Driver")

            // 创建连接
            val connectionStringWithParams = appendConnectionParams(config.connectionString)
            connection = DriverManager.getConnection(connectionStringWithParams)
            isConnected = true

            logger.info { "Connected to MySQL successfully" }
            true
        } catch (e: Exception) {
            logger.error(e) { "Failed to connect to MySQL: ${e.message}" }
            false
        }
    }

    override suspend fun write(ctx: Context, batch: List<Message>): WriteResult = withContext(Dispatchers.IO) {
        if (!isConnected || connection == null) {
            throw IllegalStateException("Not connected to MySQL")
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
                    logger.error(e) { "Error writing message to MySQL: ${e.message}" }
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

            logger.error(e) { "Error writing batch to MySQL: ${e.message}" }

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
        logger.info { "Closing MySQL output connector" }
        try {
            connection?.close()
        } catch (e: Exception) {
            logger.error(e) { "Error closing MySQL connection: ${e.message}" }
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
            is Boolean -> if (value) "1" else "0"  // MySQL 使用 1/0 表示布尔值
            is Date -> "'${value}'"
            else -> "'${value.toString().replace("'", "''")}'"
        }
    }

    private fun maskConnectionString(connectionString: String): String {
        // 掩码处理，隐藏密码
        return connectionString.replace(Regex("password=([^&]*)"), "password=****")
            .replace(Regex("pwd=([^&]*)"), "pwd=****")
    }

    private fun appendConnectionParams(connectionString: String): String {
        val params = mutableListOf<String>()
        
        // 添加 SSL 参数
        params.add("useSSL=${config.useSSL}")
        
        // 添加公钥检索参数
        params.add("allowPublicKeyRetrieval=${config.allowPublicKeyRetrieval}")
        
        // 检查连接字符串是否已包含参数
        return if (connectionString.contains("?")) {
            "$connectionString&${params.joinToString("&")}"
        } else {
            "$connectionString?${params.joinToString("&")}"
        }
    }
}

/**
 * MySQL 连接器工厂
 */
class MySQLConnectorFactory : ConnectorFactory {
    override fun createInput(config: Config): Input {
        return MySQLInputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }

    override fun createOutput(config: Config): Output {
        return MySQLOutputConnector().apply {
            kotlinx.coroutines.runBlocking { configure(config) }
        }
    }
}
