package com.sfl.flink.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;


public class Test04 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(2);

        //  3 ，读数据 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        //  4 ，打印 ：
        dsSource.print();

        //  5 ，执行 ：
        env.execute();
    }
}