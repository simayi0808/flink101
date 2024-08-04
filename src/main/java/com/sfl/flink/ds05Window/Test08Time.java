package com.sfl.flink.ds05Window;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWs02;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

//  开窗 ：key，时间，华东窗口
public class Test08Time {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //      1.1，并行度：1
        env.setParallelism(1);
        //      1.2，时间语义：默认-事件时间

        //  2 ，源 ：数据生成器，生成几个数，生成速度【每秒1条】，数据类型
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWs02(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.POJO(WaterSensor.class));

        //  3 ，读数据 ：源，水印策略，流名字
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  4 ，设置 ：水印策略
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
        //  4 ，设置 ：水印【事件时间-才会有水印】
        SingleOutputStreamOperator<WaterSensor> dsWm = dsSource.assignTimestampsAndWatermarks(watermarkStrategy);

        //  5 ，首次处理 ：无逻辑，就为了看看数据
//        SingleOutputStreamOperator<WaterSensor> process01 = dsWm.process(new ProcessFunction<WaterSensor, WaterSensor>() {
//            @Override
//            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
//                System.out.println("数据：" + value.toString());
//                System.out.println("时间水印：" + ctx.timerService().currentWatermark());
//                System.out.println("数据时间：" + ctx.timestamp());
//                out.collect(value);
//            }
//        });

        //  5 ，开窗 ：5s 滚动窗口
        AllWindowedStream<WaterSensor, TimeWindow> dsWined = dsWm.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        //  6 ，逻辑 ：
        SingleOutputStreamOperator<String> dsRes = dsWined.process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<WaterSensor> iterableElements, Collector<String> out) throws Exception {
                //  1 ，数据 ：
                List<Integer> dataList = IteratorUtils.toList(iterableElements.iterator());
                //  2 ，时间水印 ：
                //  3 ，窗口 ：
                TimeWindow window = context.window();
                long start = window.getStart();
                long end = window.getEnd();
                long maxTime = window.maxTimestamp();
                out.collect(start + " --> " + end + "：【" + maxTime + "】" +dataList.toString());
            }
        });

        //  8 ，打印 ：
        dsRes.print();
        //  9 ，执行 ：
        env.execute();
    }

}
