package com.sfl.flink.flat;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//  1 ，作用 ：将一行打散为多行
//  2 ，泛型 ：输入，输出
public class SpaceFlat implements FlatMapFunction<String, Tuple2<String, Long>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        //  1 ，字符串切割，用空格切
        String[] arr = value.split(" ");
        //  2 ，每个单词，回收一次，回收为二元组
        for (int i = 0; i < arr.length; i++) {
            out.collect(new Tuple2<String, Long>(arr[i],1l));
        }
    }
}
