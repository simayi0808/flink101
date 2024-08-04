package com.sfl.flink.ds02Part;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Test05 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> dsSource02 = env.fromElements("6", "7", "8", "9", "10");

        //  4 ，合并 ：
        ConnectedStreams<Integer, String> dsConnect = dsSource01.connect(dsSource02);

        SingleOutputStreamOperator<Integer> dsRes = dsConnect.map(new CoMapFunction<Integer, String, Integer>() {
            @Override
            public Integer map1(Integer value) throws Exception {
                return value;
            }
            @Override
            public Integer map2(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        //  5 ，打印 ：
        dsRes.print();

        //  6 ，执行 ：
        env.execute();
    }

}
