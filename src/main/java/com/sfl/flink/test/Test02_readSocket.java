package com.sfl.flink.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test02_readSocket {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  2 ，源 ：nc 源
        DataStreamSource<String> dsSource = env.socketTextStream("cls02", 7890);
        //  3 ，一行转多行 ：
        SingleOutputStreamOperator<Tuple2<String, Long>> dsWords = dsSource.flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
            //  1 ，字符串切割，用空格切
            String[] arr = value.split(" ");
            //  2 ，每个单词，回收一次，回收为二元组
            for (int i = 0; i < arr.length; i++) {
                out.collect(new Tuple2<String, Long>(arr[i], 1l));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //  4 ，分组，聚合 ：
        SingleOutputStreamOperator<Tuple2<String, Long>> dsRes = dsWords.keyBy(tp2 -> tp2.f0).sum(1);
        //  5 ，打印 ：
        dsRes.print();
        //  6 ，执行 ：
        env.execute();
    }
}