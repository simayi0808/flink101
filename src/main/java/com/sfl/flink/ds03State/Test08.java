package com.sfl.flink.ds03State;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class Test08 {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //  2 ，源 ：
        DataStreamSource<String> dsSource01 = env.socketTextStream("cls01", 7777);
        //  3 ，分组 ：都到同一组中
        KeyedStream<String, Integer> dsKeyed = dsSource01.keyBy(e -> 1);
        //  4 ，统计 ：每分钟多少次访问
        //      泛型 ：k，输入，输出
        SingleOutputStreamOperator<String> dsRes = dsKeyed.process(new KeyedProcessFunction<Integer, String, String>() {
            //  1 ，状态 ：定义
            ValueState<Integer> numState;
            //  2 ，状态 ：初始化，存活-1分钟
            @Override
            public void open(Configuration parameters) throws Exception {
                //  初始化状态 ：
                ValueStateDescriptor<Integer> lastVcStateDiscripor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                numState = getRuntimeContext().getState(lastVcStateDiscripor);

            }
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                out.collect(value);
            }
        });

        //  6 ，打印 ：
        dsRes.print();
        //  7 ，执行 ：
        env.execute();
    }

}
