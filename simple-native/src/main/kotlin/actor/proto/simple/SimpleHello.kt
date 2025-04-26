package actor.proto.simple

/**
 * 一个非常简单的示例，不依赖于 ProtoActor 库
 */
fun main() {
    println("Hello, Native World!")
    println("This is a simple example that doesn't depend on ProtoActor.")
    
    // 测试一些基本的 Kotlin 功能
    val numbers = listOf(1, 2, 3, 4, 5)
    val sum = numbers.sum()
    println("Sum of $numbers is $sum")
    
    // 测试 lambda 表达式
    val doubled = numbers.map { it * 2 }
    println("Doubled: $doubled")
    
    // 测试字符串模板
    val name = "Native"
    println("Hello, $name!")
    
    println("Simple example completed!")
}
