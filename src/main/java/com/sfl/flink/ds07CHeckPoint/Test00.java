package com.sfl.flink.ds07CHeckPoint;

import com.alibaba.fastjson.JSONObject;
import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWaterSensor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test00 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，env 设置 ：
        CheckpointConfig configCheck = env.getCheckpointConfig();

        //      2.1 ，设置并行度 ：
        env.setParallelism(1);

        //      2.2 ，检查点 ：开，5s保存一次，精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //      2.3 ，检查点 ：持久化数据，保存位置【可以是 localFile，也可以 hdfs】
        configCheck.setCheckpointStorage("hdfs://cls01:9820/flink/test01/chk");
        //      2.4 ，检查点 ：超时时间，太久了保存不进，就不保存了，丢弃此状态，默认【10分钟】
        configCheck.setCheckpointTimeout(60000);
        //      2.5 ，同时又几个检查点 id ，在运行 ：
        configCheck.setMaxConcurrentCheckpoints(2);
        //      2.6 ，检查点间隔 ：上一轮检查点结束，到下一轮检查点开始，间隔多久
        configCheck.setMinPauseBetweenCheckpoints(1000);
        //      2.7 ，正常停止作业 ：检查点数据，是否保留【我们设置为：保留】
        configCheck.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //      2.8 ，允许 ck 连续失败次数，超过限制，程序停止，默认【0】
        configCheck.setTolerableCheckpointFailureNumber(4);

        //  3 ，数据源 ：数据生成器，生成几个数，生成速度【每秒2条】，数据类型，
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWaterSensor(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(2), Types.POJO(WaterSensor.class));

        //  4 ，读数据 ：源，水印策略，流名字
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  5 ，转换 ：
        SingleOutputStreamOperator<String> dsStr = dsSource.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                String s = JSONObject.toJSONString(value);
                return s;
            }
        });

        //  5 ，打印 ：
        dsStr.print();

        //  6 ，执行 ：
        env.execute();

    }

}
