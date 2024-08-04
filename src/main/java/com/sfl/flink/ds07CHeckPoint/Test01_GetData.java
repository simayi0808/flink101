package com.sfl.flink.ds07CHeckPoint;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class Test01_GetData {

    public static void main(String[] args) throws Exception {

        //  0 ，系统环境设置 ：hadoop 用户
        System.setProperty("HADOOP_USER_NAME", "keen");

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，env 设置 ：
        CheckpointConfig configCheck = env.getCheckpointConfig();

        //      2.1 ，设置并行度 ：
        env.setParallelism(3);
        //      2.2 ，检查点设置 ：
        //          1 ，检查点 ：开，5s保存一次，精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //          2 ，检查点 ：持久化数据，保存位置【可以是 localFile，也可以 hdfs】
        configCheck.setCheckpointStorage("hdfs://cls01:9820/flink/test01/chk/producer");
        //          3 ，外部保存设置 ：一致存在，不删除【如果需要，手工删除】
        configCheck.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //          4 ，时间过期后，这个状态就不保存了，重试
        configCheck.setCheckpointTimeout(30000);
        //          5 ，连续重试 10 次，如果还不能保存状态的话，程序停止
        configCheck.setTolerableCheckpointFailureNumber(2);
        //          6 ，同时允许几个【检查点id】并行运行保存任务【前一轮数据的下游检查点还没结束，下一轮数据的上游检查点已经开始】
        configCheck.setMaxConcurrentCheckpoints(2);
        //          7 ，最小等待时间 ：上一次检查点结束后，至少多少秒，下一轮检查点菜可以开始
        configCheck.setMinPauseBetweenCheckpoints(1000);

        //  2 ，输入 ：socket 【服务端：nc -lk 7777】
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，输出 ：
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //  指定 kafka 的地址和端口
                .setBootstrapServers("cls01:9092,cls02:9092,cls03:9092")
                //  指定序列化器：指定 Topic 名称、具体的序列化
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                byte[] key = element.getBytes(StandardCharsets.UTF_8);
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("sfl_test", key, value);
                            }
                        }
                )
                //  写到 kafka 的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //  如果是精准一次，必须设置，事务的前缀
                .setTransactionalIdPrefix("sfl_")
                //  如果是精准一次，必须设置，事务超时时间 : 大于 checkpoint 间隔，小于 max 15分钟【这里设置：10分钟】
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 2*60*1000+"")
                .build();

        //  5 ，打印 ：
        dsSource.sinkTo(kafkaSink);
        //  6 ，执行 ：
        env.execute();
    }
}