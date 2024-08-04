package com.sfl.flink.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;


public class Test08_kafka {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //      开启检查点，为了 kafka 的精确一次性
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        //  2 ，设置并行度 ：
        env.setParallelism(4);

        //  3 ，数据源 ：自定义
        DataStreamSource<Integer> dsSource = env.fromElements(11, 12, 13, 14, 15, 16, 17, 18, 19);
        SingleOutputStreamOperator<String> dsRes = dsSource.map((Integer i) -> i + "");

        //  4 ，kafka 输出 ：
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("cls01:9092,cls02:9092,cls03:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
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
                //  如果是精准一次，必须设置，事务超时时间 : 大于 checkpoint 间隔，小于 max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();

        //  5 ，打印 ：
        dsRes.sinkTo(kafkaSink);

        //  6 ，执行 ：
        env.execute();
    }
}