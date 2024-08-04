package com.sfl.flink.ds02Part;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test02 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        //  4 ，分流 ：
        SingleOutputStreamOperator<Integer> dsRes01 = dsSource.filter(e -> e % 2 == 0);
        SingleOutputStreamOperator<Integer> dsRes02 = dsSource.filter(e -> e % 2 != 0);

        //  5 ，输出 ：
        dsRes01.print("偶数：");
        dsRes02.print("奇数：");

        //  6 ，执行 ：
        env.execute();

    }

}
