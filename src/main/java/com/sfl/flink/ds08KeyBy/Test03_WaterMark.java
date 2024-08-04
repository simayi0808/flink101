package com.sfl.flink.ds08KeyBy;

import com.sfl.flink.source.GenWs06_Num;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class Test03_WaterMark {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  最大并行度
        //  env.setMaxParallelism(3);
        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：数据生成器，生成几个数，生成速度【每秒 1 条】，数据类型，
        DataGeneratorSource<Long> sourceLong = new DataGeneratorSource<>(new GenWs06_Num(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.LONG);

        //  4 ，读数据 ：
        DataStreamSource<Long> dsSource = env.fromSource(sourceLong, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  5 ，设置 ：水印策略
        WatermarkStrategy<Long> watermarkStrategy = WatermarkStrategy
                //  1.1 ，时间水印 ：顺序数据，无乱序
                .<Long>forMonotonousTimestamps()
                //  1.2 ，提取时间字段 ：
                .withTimestampAssigner(new SerializableTimestampAssigner<Long>() {
                    @Override
                    public long extractTimestamp(Long element, long recordTimestamp) {
                        //  返回的时间戳，要【毫秒】
                        long tmLong = System.currentTimeMillis();
                        return tmLong;
                    }
                });

        //  6 ，设置 ：将水印，放到流中
        SingleOutputStreamOperator<Long> dsWm = dsSource.assignTimestampsAndWatermarks(watermarkStrategy);

        //  7 ，keyBy ：
        KeyedStream<Long, Long> dsKeyed = dsWm.keyBy(new KeySelector<Long, Long>() {
            @Override
            public Long getKey(Long value) throws Exception {
                Long k = value % 2;
                //  System.out.println(k);
                return k;
            }
        });

        //  8 ，开窗 ： 5s 一个，滚动窗口
        WindowedStream<Long, Long, TimeWindow> dsWin = dsKeyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //  9 ，处理 ：
        SingleOutputStreamOperator<Long> dsRes = dsWin.process(new ProcessWindowFunction<Long, Long, Long, TimeWindow>() {
            @Override
            public void process(Long aLong, Context context, Iterable<Long> elements, Collector<Long> out) throws Exception {
                List list = IteratorUtils.toList(elements.iterator());
                System.out.println(list);
                out.collect(0l);
            }
        });

        //  7 ，打印 ：
        dsRes.print();

        //  6 ，执行 ：
        env.execute();

    }

}
