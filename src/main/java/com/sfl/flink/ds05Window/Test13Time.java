package com.sfl.flink.ds05Window;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.par.MyPartitioner;
import com.sfl.flink.source.GenWs02;
import com.sfl.flink.source.GenWs04;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

//  开窗 ：分组，是否可以让水印等待【水印隔离，是分区隔离，还是分组隔离】
public class Test13Time {
    public static void main(String[] args) throws Exception {
        //  1 ，环境 ：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  2 ，源 ：
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWs04(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.POJO(WaterSensor.class));
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  3 ，
        // 自定义分区器：数据%分区数，只输入奇数，都只会去往map的一个子任务
        SingleOutputStreamOperator<WaterSensor> socketDS = dsSource
                .partitionCustom(new MyPartitioner(), r -> r.getId() + "")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r.getTs())
                                .withIdleness(Duration.ofSeconds(5))  //空闲等待5s
                );

        // 分成两组： 奇数一组，偶数一组 ， 开10s的事件时间滚动窗口
        socketDS
                .keyBy(r -> r.getId() % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                })
                .print();

        dsSource.print();
        env.execute();
    }
}
