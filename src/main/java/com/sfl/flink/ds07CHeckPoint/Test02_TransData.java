package com.sfl.flink.ds07CHeckPoint;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.List;

public class Test02_TransData {

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
        configCheck.setCheckpointStorage("hdfs://cls01:9820/flink/test01/chk/consumer");
        //          3 ，外部保存设置 ：一致存在，不删除【如果需要，手工删除】
        configCheck.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //          4 ，时间过期后，这个状态就不保存了，重试
        configCheck.setCheckpointTimeout(60000);
        //          5 ，连续重试 10 次，如果还不能保存状态的话，程序停止
        configCheck.setTolerableCheckpointFailureNumber(10);
        //          6 ，同时允许几个【检查点id】并行运行保存任务【前一轮数据的下游检查点还没结束，下一轮数据的上游检查点已经开始】
        configCheck.setMaxConcurrentCheckpoints(2);
        //          7 ，最小等待时间 ：上一次检查点结束后，至少多少秒，下一轮检查点菜可以开始
        configCheck.setMinPauseBetweenCheckpoints(1000);

        //          8 ，检查点存储位置 ：一般不手动指定，如果手动指定，【注意，目录不要重复】
        //  configCheck.setCheckpointStorage("hdfs:///checkpoints-data/");

        //  3 ，数据源 ：kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //  1 ，地址
                .setBootstrapServers("cls01:9092,cls02:9092,cls03:9092")
                //  2 ，主题
                .setTopics("sfl_test")
                //  3 ，消费者组
                .setGroupId("sfl")
                //  4 ，从哪里开始消费 ：【kafka原生：无论从哪开始，重新开始都是从上次开始；flink自定：最早就是最早，最晚就是最晚。】
                //      flink 自定义的规则 ：最早或【上次位置】
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //  4 ，读数据 ：
        DataStreamSource<String> sourceKafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        //  5 ，逻辑 ：
        //      5.1，分组 ：
        KeyedStream<String, Integer> dsKeyed = sourceKafka.keyBy(e -> Integer.parseInt(e) % 3);
        //      6.2，数据搜集 ：【泛型：k，in，out】
        SingleOutputStreamOperator<String> dsRes = dsKeyed.process(new KeyedProcessFunction<Integer, String, String>() {
            //  1 ，定义状态 ：
            private ListState<String> listState;
            //  2 ，初始化状态 ：
            @Override
            public void open(Configuration parameters) throws Exception {
                //  初始化状态 ：
                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("listState", Types.STRING));
            }
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                listState.add(value);
                Iterable<String> iterbValues = listState.get();
                List list = IteratorUtils.toList(iterbValues.iterator());
                out.collect(list.toString());
            }
        });
        //  5 ，打印 ：
        dsRes.print();
        //  6 ，执行 ：
        env.execute();
    }
}
