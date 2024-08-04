package com.sfl.flink.ds02Part;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test04 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> dsSource02 = env.fromElements(6, 7, 8, 9, 10);
        DataStreamSource<Integer> dsSource03 = env.fromElements(11,12,13,14,15);

        //  4 ，合并 ：
        DataStream<Integer> dsRes = dsSource01.union(dsSource02, dsSource03);

        //  5 ，打印 ：
        dsRes.print();

        //  6 ，执行 ：
        env.execute();
    }

}
