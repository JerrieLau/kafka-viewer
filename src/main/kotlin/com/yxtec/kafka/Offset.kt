package com.yxtec.kafka

/**
 * Created by jerrie on 17-6-5.
 */
data class Offset(
        var topic: String,
        var partition: Int,
        var logSize: Long = 0,
        var consumerOffset: Long = 0,
        var lag: Long = 0
)