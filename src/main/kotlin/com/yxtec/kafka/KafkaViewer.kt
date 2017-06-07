package com.yxtec.kafka

import com.google.gson.Gson
import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.consumer.SimpleConsumer
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.ZooKeeper
import java.io.IOException
import java.io.UnsupportedEncodingException
import java.nio.charset.Charset
import java.util.*

/**
 * Kafka Viewer Single Instance
 * @author :[刘杰](mailto:liujie@ebnew.com)
 */
object KafkaViewer {

    /**
     * The constant connectStr.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    private lateinit var connectStr: String
    /**
     * The constant topic.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    private lateinit var topic: String
    /**
     * The constant consumerGroup.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    private var consumerGroup: String? = null

    /**
     * The constant zooKeeper.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    private lateinit var zooKeeper: ZooKeeper

    /**
     * The constant offsetMap.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    private val offsetMap = HashMap<Int, Offset>()


    @JvmStatic fun main(args: Array<String>?) {
        //检测参数
        checkArgs(args)

        createZkConnect()

        //获取topic分区信息
        getTopicPartitionMsg()

        //获取消费者消费位置
        getConsumerOffsetMsg()

        //获取logSize
        getLogSize()

        //计算Lag
        calculateLag()

        //打印结果
        printOffsetCheckMsg()
    }


    private fun createZkConnect() {
        try {
            zooKeeper = ZooKeeper(connectStr, 600000, null)
        } catch (e: IOException) {
            throw IllegalArgumentException("按指定的zookeeper.connect不能建立连接，请检查指定的zookeeper.connect是否正确，请参考kafka中的zookeeper.connect参数!")
        }

    }

    /**
     * Check args.

     * @param args the args
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:49:52
     */
    private fun checkArgs(args: Array<String>?) {
        if (args == null || args.size < 2) {
            throw IllegalArgumentException("请指定正确的参数，需要指定zookeeper连接信息（kafka中的zookeeper.connect参数）、topic(消息主题)、[消费者分组]。" +
                    "\n格式: java -jar kafka-viewer-1.0-SNAPSHOT-jar-with-dependencies.jar zookeeper连接信息 消息主题 消费者分组" +
                    "\n示例: java -jar kafka-viewer-1.0-SNAPSHOT-jar-with-dependencies.jar 10.4.0.74:2181,10.4.0.75:2181,10.4.0.76:2181/dckafka bidlink_canal" +
                    "\n示例: java -jar kafka-viewer-1.0-SNAPSHOT-jar-with-dependencies.jar 10.4.0.74:2181,10.4.0.75:2181,10.4.0.76:2181/dckafka bidlink_canal dc-daemon-test")
        }
        connectStr = args[0]
        topic = args[1]

        if (args.size > 2) {
            consumerGroup = args[2]
        }
    }

    /**
     * Print offset check msg.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:26:01
     */
    private fun printOffsetCheckMsg() {
        println("\n\n===================检测结果=================")
        println("分区\t消息总数(条)\t已消费数(条)\t积压数(条)")
        var totalSize: Long = 0
        var totalConsumedSize: Long = 0
        for ((_, partition, logSize, consumerOffset, lag) in offsetMap.values) {
            totalSize += logSize
            totalConsumedSize += consumerOffset

            println(String.format("%3d\t%8d\t%8d\t%8d", partition, logSize, consumerOffset, lag))
        }
        System.out.printf("\n\n汇总: 消息总共%d条，已消费%d条，积压%d条.\n", totalSize, totalConsumedSize, totalSize - totalConsumedSize)
    }

    /**
     * Calculate lag.

     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:24:33
     */
    private fun calculateLag() {
        for (offset in offsetMap.values) {
            offset.lag = offset.logSize - offset.consumerOffset
        }
    }

    /**
     * Gets log size.

     * @throws KeeperException              the keeper exception
     * *
     * @throws InterruptedException         the interrupted exception
     * *
     * @throws UnsupportedEncodingException the unsupported encoding exception
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:21:45
     */
    @Throws(KeeperException::class, InterruptedException::class, UnsupportedEncodingException::class)
    private fun getLogSize() {
        val path = "/brokers/ids"
        zooKeeper.exists(path, false) ?: throw IllegalArgumentException("从\"$connectStr\"上查询出brokers的实例节点不存在，请检查指定的zookeeper.connect连接地址是否正确，zookeeper.connect必须跟kafka配置文件server.xml中的zookeeper.connect一致!")

        val brokers: List<String> = zooKeeper.getChildren(path, false, null)
        val gson = Gson()
        loop@ for (brokerId in brokers) {
            //获取broker信息
            val data = zooKeeper.getData("/brokers/ids/" + brokerId, false, null)

            val brokerStr = data.toString(Charset.forName("UTF-8"))
            val broker: Broker = gson.fromJson(brokerStr, Broker::class.java)

            for (offset in offsetMap.values) {
                val logSize = getLogSize(broker.host, broker.port, topic, offset.partition)
                offset.logSize = logSize
            }
            break@loop
        }
    }

    /**
     * Gets consumer offset msg.

     * @throws KeeperException              the keeper exception
     * *
     * @throws InterruptedException         the interrupted exception
     * *
     * @throws UnsupportedEncodingException the unsupported encoding exception
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:19:04
     */
    @Throws(KeeperException::class, InterruptedException::class, UnsupportedEncodingException::class)
    private fun getConsumerOffsetMsg() {

        consumerGroup?.let {
            val path = "/consumers/$consumerGroup/offsets/$topic"
            zooKeeper.let {
                zooKeeper.exists(path, false) ?: throw IllegalArgumentException("从\"$connectStr\"上根据指定的消息费者组（$consumerGroup）查询出Topic（$topic）的节点不存在，请核对消费者分组是否正确!")
                val consumerPartitions = zooKeeper.getChildren(path, false, null) ?: throw IllegalArgumentException("从\"$connectStr\"上根据指定的消息费者组（$consumerGroup）查询出Topic（$topic）的消费者位置为空，请核对消费者分组是否正确!")
                for (consumerPartition in consumerPartitions) {
                    val subPath = "/consumers/$consumerGroup/offsets/$topic/$consumerPartition"
                    val data = zooKeeper.getData(subPath, false, null)

                    val consumerOffset = data.toString(Charset.forName("UTF-8"))
                    val offset: Offset? = offsetMap[consumerPartition.toInt()]
                    offset?.let { offset.consumerOffset = java.lang.Long.valueOf(consumerOffset) } ?: throw IllegalArgumentException("从\"$connectStr\"上的消息费者组（$consumerGroup）查询出的分区（$consumerPartition）在Broker下的分区列表中未找到，请检查是否重新进行了分区!")
                }
            }
        }
    }

    /**
     * Gets topic partition msg.

     * @throws KeeperException      the keeper exception
     * *
     * @throws InterruptedException the interrupted exception
     * *
     * @author :[刘杰](mailto:liujie@ebnew.com)
     * *
     * @date :2017-05-25 13:17:42
     */
    @Throws(KeeperException::class, InterruptedException::class)
    private fun getTopicPartitionMsg() {
        zooKeeper.let {
            val path = "/brokers/topics/$topic/partitions"
            zooKeeper.exists(path, false) ?: throw IllegalArgumentException("从\"$connectStr\"中查询Topic（$topic）的分区节点不存在，请检查Topic是否正确!")

            val partitionList = zooKeeper.getChildren(path, false, null)
            for (partitionStr in partitionList) {
                val partition = Integer.valueOf(partitionStr)
                val offset = Offset(partition = partition, topic = topic)
                offsetMap.put(partition, offset)
            }
        }
    }

    /**
     * 获取kafka logSize
     * @param host
     * *
     * @param port
     * *
     * @param topic
     * *
     * @param partition
     * *
     * @return
     */
    fun getLogSize(host: String, port: Int, topic: String, partition: Int): Long {
        val clientName = "Client_" + topic + "_" + partition
        val leaderBroker = getLeaderBroker(host, port, topic, partition)
        val reaHost: String? = leaderBroker?.host() ?: throw IllegalArgumentException("从\"$connectStr\"上的brokers中获取Topic（${KafkaViewer.topic}）的分区信息为空，请检查指定的zookeeper.connect连接地址是否正确，zookeeper.connect必须跟kafka配置文件server.xml中的zookeeper.connect一致!")
        val simpleConsumer = SimpleConsumer(reaHost, port, 10000, 64 * 1024, clientName)
        val topicAndPartition = TopicAndPartition(topic, partition)
        val requestInfo = HashMap<TopicAndPartition, PartitionOffsetRequestInfo>()
        requestInfo.put(topicAndPartition, PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1))
        val request = kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName)
        val response = simpleConsumer.getOffsetsBefore(request)
        if (response.hasError()) {
            throw IllegalArgumentException("Error fetching data Offset , Reason: ${response.errorCode(topic, partition)}")
        }
        val offsets = response.offsets(topic, partition)
        return offsets[0]
    }

    /**
     * 获取broker ID
     * @param host
     * *
     * @param port
     * *
     * @param topic
     * *
     * @param partition
     * *
     * @return
     */
    fun getBrokerId(host: String, port: Int, topic: String, partition: Int): Int? {
        val leaderBroker = getLeaderBroker(host, port, topic, partition)
        return leaderBroker?.id()
    }

    /**
     * 获取leaderBroker
     * @param host
     * *
     * @param port
     * *
     * @param topic
     * *
     * @param partition
     * *
     * @return
     */
    private fun getLeaderBroker(host: String, port: Int, topic: String, partition: Int): kafka.cluster.Broker? {
        val clientName = "Client_Leader_LookUp"
        var partitionMetaData: PartitionMetadata? = null
        val consumer: SimpleConsumer = SimpleConsumer(host, port, 10000, 64 * 1024, clientName)
        val topics = ArrayList<String>()
        topics.add(topic)
        val request = TopicMetadataRequest(topics)
        val reponse = consumer.send(request)
        val topicMetadataList = reponse.topicsMetadata()
        for (topicMetadata in topicMetadataList) {
            for (metadata in topicMetadata.partitionsMetadata()) {
                if (metadata.partitionId() == partition) {
                    partitionMetaData = metadata
                    break
                }
            }
        }
        return partitionMetaData?.leader()
    }

}