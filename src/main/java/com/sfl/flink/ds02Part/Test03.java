package com.sfl.flink.ds02Part;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test03 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        //  4 ，侧输出流 ：
        OutputTag<Integer> os01 = new OutputTag<Integer>("os01", Types.INT){};

        //  5 ，处理逻辑 ：
        SingleOutputStreamOperator<Integer> dsRes = dsSource.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                //  奇数，留在主流，偶数，侧输出流
                if (value % 2 != 0) {
                    out.collect(value);
                } else {
                    ctx.output(os01, value);
                }
            }
        });

        //  5 ，输出 ：打印
        dsRes.print("奇数：");
        SideOutputDataStream<Integer> sideOutput = dsRes.getSideOutput(os01);
        sideOutput.print("偶数：");

        //  6 ，执行 ：
        env.execute();
    }

}
