package com.sfl.flink.timer;

import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWs02;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test02 {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  2 ，设置并行度 ：
        env.setParallelism(1);
        //  3 ，数据源 ：数据生成器，生成几个数，生成速度【每秒1条】，数据类型，
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWs02(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), Types.POJO(WaterSensor.class));
        //  4 ，时间水印 ：有序时间流，
        WatermarkStrategy<WaterSensor> ws = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor e, long recordTimestamp) {
                        return e.getTs();
                    }
                });

        //  5 ，读数据 ：源，水印策略，流名字
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, ws, "waterSensor");
        //  6 ，分组 ：
        KeyedStream<WaterSensor, Integer> dsKeyed = dsSource.keyBy(e -> e.getId() % 2);
        //  7 ，逻辑 ：定时器【5s的时候触发】
        //      泛型 ：k ，输入，输出
        SingleOutputStreamOperator<String> dsRes = dsKeyed.process(new KeyedProcessFunction<Integer, WaterSensor, String>() {
            Integer end = 0;
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //  当前数据时间 ：
                Long ts = ctx.timestamp();
                //  变成秒，把 5x 后的啰嗦，减掉，+ 5s【5000ms】
                long end = ts - ts % 5000 + 4999;
                //  注册 ：定时器
                ctx.timerService().registerEventTimeTimer(end);
                //  下传 ：数据
                out.collect(value.toString());
            }
            //  定时器 ：触发
            //      泛型 ：开火时间，上下文，搜集器
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("定时器，触发：" + timestamp);
                //  销毁定时器 ：
                ctx.timerService().deleteEventTimeTimer(end);
            }
        });

        //  5 ，打印 ：
        dsRes.print();
        //  6 ，执行 ：
        env.execute();
    }

}
