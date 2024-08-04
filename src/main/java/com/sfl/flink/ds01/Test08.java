package com.sfl.flink.ds01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test08 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(2);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //  4 ，随机分区 ：
        DataStream<Integer> dsResp = dsSource.broadcast();

        //  5 ，打印 ：
        dsResp.print();

        //  6 ，执行 ：
        env.execute();

    }

}
