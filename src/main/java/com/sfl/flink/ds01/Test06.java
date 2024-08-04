package com.sfl.flink.ds01;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test06 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(2);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //  4 ，分组 ：奇偶数
        KeyedStream<Integer, Integer> dsKeyed = dsSource.keyBy(i -> i%2);

        //  5 ，聚合 ：
        SingleOutputStreamOperator<Integer> dsReduced = dsKeyed.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {

                //  System.out.println(value1 + "，" + value2);
                return value1 + value2;
            }
        });

        //  5 ，打印 ：
        dsReduced.print();

        //  6 ，执行 ：
        env.execute();

    }

}
