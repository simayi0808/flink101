package com.sfl.flink.ds05Window;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWs02;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//  开窗 ：key，时间，华东窗口
public class Test07Time {

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
                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        return element.getTs();
                    }
                });
        //  4 ，设置 ：水印
        SingleOutputStreamOperator<WaterSensor> dsWm = dsSource.assignTimestampsAndWatermarks(watermarkStrategy);

        //  5 ，逻辑 ：
        SingleOutputStreamOperator<String> dsRes = dsWm.process(new ProcessFunction<WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("当前时间戳：" + ctx.timestamp());
                System.out.println("当前时间水印：" + ctx.timerService().currentWatermark());

                out.collect(value.toString());
            }
        });

        //  8 ，打印 ：
        dsRes.print();
        //  9 ，执行 ：
        env.execute();
    }

}
