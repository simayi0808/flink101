package com.sfl.flink.test;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


public class Test06_kafka {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(2);

        //  3 ，数据源 ：kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cls01:9092")
                .setTopics("sfl_test")
                .setGroupId("sfl")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //  4 ，读数据 ：源，水印策略，流名字
        DataStreamSource<String> dsWs = env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3)), "waterSensor");

        //  5 ，打印 ：
        dsWs.print();

        //  6 ，执行 ：
        env.execute();
    }
}