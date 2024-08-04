package com.sfl.flink.ds05Window;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWs02;
import com.sfl.flink.source.GenWs03;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

//  开窗 ：key，时间，华东窗口
public class Test10Time {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //      1.1，并行度：1
        env.setParallelism(1);
        //      1.2，时间语义：默认-事件时间

        //  2 ，源 ：数据生成器，生成几个数，生成速度【每秒1条】，数据类型
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWs03(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.POJO(WaterSensor.class));
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  Watermark
        //  Watermark

        //  3 ，水印 ：
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                //  1.1 ，时间水印 ：顺序数据，无乱序
                .<WaterSensor>forMonotonousTimestamps()
                //  1.2 ，提取时间字段 ：
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        //  返回的时间戳，要【毫秒】
                        //  System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        return element.getTs();
                    }
                });
        SingleOutputStreamOperator<WaterSensor> dsWm = dsSource.assignTimestampsAndWatermarks(watermarkStrategy);

        //  4 ，分组 ：
        KeyedStream<WaterSensor, Integer> dsKeyed = dsWm.keyBy(e -> e.getId() % 3);

        //  5 ，开窗 ：滚动窗口
        WindowedStream<WaterSensor, Integer, TimeWindow> dsWined = dsKeyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //  6 ，逻辑 ：
        //      泛型 ：输入，输出，k ，窗口类型
        SingleOutputStreamOperator<String> dsRes = dsWined.process(new ProcessWindowFunction<WaterSensor, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<WaterSensor> iterableElements, Collector<String> out) throws Exception {
                //  获取数据 ：
                List<WaterSensor> dataList = IteratorUtils.toList(iterableElements.iterator());
                //  水印 ：wm
                long waterMarkTime = context.currentWatermark();
                //  窗口 ：
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                long maxTime = window.maxTimestamp();
                //  打印 ：中间结果 ：
                System.out.println("水印时间：" + waterMarkTime);
                out.collect(start + "-->" + end + "，窗内允许的最大时间【" + maxTime + "】" + dataList.toString());
            }
        });

        dsSource.print("源：");
        //  9 ，执行 ：
        env.execute();
    }

}
