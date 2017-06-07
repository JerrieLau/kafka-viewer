package com.yxtec.kafka

/**
 * Created by jerrie on 17-6-5.
 */
data class Broker(
        var host: String,
        var port: Int,
        var version: Int,
        var timestamp: Long,
        var jmxPort: Int
)