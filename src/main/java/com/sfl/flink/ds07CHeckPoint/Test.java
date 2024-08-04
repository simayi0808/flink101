package com.sfl.flink.ds07CHeckPoint;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

public class Test {

    public static void main(String[] args) throws Exception {

        //  0 ，系统环境设置 ：hadoop 用户
        System.setProperty("HADOOP_USER_NAME", "keen");

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，输入 ：socket 【服务端：nc -lk 7777】
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，输出 ：
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //  指定 kafka 的地址和端口
                .setBootstrapServers("cls01:9092,cls02:9092,cls03:9092")
                //  指定序列化器：指定 Topic 名称、具体的序列化
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("sfl_test")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                // 写到kafka的一致性级别： 精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 如果是精准一次，必须设置 事务的前缀
                .setTransactionalIdPrefix("sfl_test_")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 2*60*1000+"")
                .build();

        //  5 ，打印 ：
        dsSource.sinkTo(kafkaSink);
        //  6 ，执行 ：
        env.execute();
    }
}