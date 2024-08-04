package com.sfl.flink.ds01;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test07 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        SingleOutputStreamOperator<Integer> map = dsSource.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(index + "：" + value);
                return value;
            }
        });

        //  4 ，分组 ：奇偶数
        KeyedStream<Integer, Integer> dsKeyed = map.keyBy(i -> i%2);


        //  5 ，聚合 ：
        SingleOutputStreamOperator<Integer> dsReduced = dsKeyed.reduce(new RichReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                RuntimeContext context = getRuntimeContext();


                int index = context.getIndexOfThisSubtask();
                System.out.println(index + "：" + value1 + "--" + value2);
                return value1 + value2;
            }
        });

        //  5 ，打印 ：
        dsReduced.print();

        //  6 ，执行 ：
        env.execute();

    }

}
