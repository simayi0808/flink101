package com.sfl.flink.ds03State;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test05 {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //  2 ，源 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24);

        //  3 ，分组 ：
        KeyedStream<Integer, Integer> dsKeyed = dsSource01.keyBy(e -> e % 5);

        //  4 ，处理 ：
        //      泛型 ：k ，输入，输出
        SingleOutputStreamOperator<Integer> dsRes = dsKeyed.process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            //  1 ，定义状态 ：
            ValueState<Integer> numState;
            //  2 ，初始化状态 ：
            @Override
            public void open(Configuration parameters) throws Exception {
                //  初始化状态 ：
                numState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
            }
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                //  1 ，获取 ：
                Integer num = numState.value();
                //  2 ，计数 ：
                if(num == null){
                    num=1;
                }else{
                    num++;
                }
                //  3 ，设置回去 ：
                numState.update(num);
                //  4 ，打印 ：
                //      key
                Integer currentKey = ctx.getCurrentKey();
                //      子任务编号：每个算子都有一堆子任务，不重要
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                //      子任务并行度：
                int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

                System.out.println(
                        "para：" + numberOfParallelSubtasks +
                        "，subTask：" + indexOfThisSubtask +
                        "，key："+currentKey +
                        "，value：" + value +
                        "，共几个：" + num
                );
                out.collect(value);
            }
        });

        //  6 ，打印 ：
        dsRes.print();
        //  7 ，执行 ：
        env.execute();
    }

}
