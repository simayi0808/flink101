package com.sfl.flink.ds08KeyBy;

import com.sfl.flink.source.GenWs06_Num;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test02_KeyGroup {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  最大并行度
        env.setMaxParallelism(3);
        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：数据生成器，生成几个数，生成速度【每秒 1 条】，数据类型，
        DataGeneratorSource<Long> sourceLong = new DataGeneratorSource<>(new GenWs06_Num(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.LONG);

        //  4 ，读数据 ：
        DataStreamSource<Long> dsSource = env.fromSource(sourceLong, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  5 ，扩展并行度 ：
        SingleOutputStreamOperator<Long> dsMap = dsSource.map(e -> e).setParallelism(3);

        //  6 ，keyBy ：
        KeyedStream<Long, Long> dsKeyed = dsMap.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long value) throws Exception {
                Long k = value%3;
                //  System.out.println(k);
                return k;
            }
        });

        //  7 ，打印 ：
        dsKeyed.print().setParallelism(3);

        //  6 ，执行 ：
        env.execute();

    }

}
