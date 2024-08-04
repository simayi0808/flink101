package com.sfl.flink.ds02Part;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        //  4 ，map ：
        SingleOutputStreamOperator<Integer> ds01 = dsSource.map(e -> e).setParallelism(3);

        //  5 ，自定义分区 ：
        DataStream<Integer> ds02 = ds01.partitionCustom(new SflPertitioner(), e -> e);

        //  5 ，打印 ：
        ds02.print().setParallelism(3);

        //  6 ，执行 ：
        env.execute();

    }

}
