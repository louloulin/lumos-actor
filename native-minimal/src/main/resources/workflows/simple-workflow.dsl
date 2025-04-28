workflow("simple-workflow") {
    val input = input("file") {
        path = "input/sample.csv"
        format = "csv"
    }

    val processor = processor("mapping") {
        mapping = ".processed = true"
    }

    val output = output("file") {
        path = "output/processed.csv"
        format = "csv"
    }

    connect(input to processor)
    connect(processor to output)
}
