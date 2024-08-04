package com.sfl.flink.ds03State;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test06 {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //  2 ，源 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24);

        //  3 ，分组 ：
        KeyedStream<Integer, Integer> dsKeyed = dsSource01.keyBy(e -> e % 5);

        //  4 ，处理 ：
        //      泛型 ：k ，输入，输出
        SingleOutputStreamOperator<Integer> dsRes = dsKeyed.process(new KeyedProcessFunction<Integer, Integer, Integer>() {
            //  1 ，定义状态 ：
            ListState<Integer> listState;
            //  2 ，初始化状态 ：
            @Override
            public void open(Configuration parameters) throws Exception {
                //  初始化状态 ：
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Types.INT));
            }
            @Override
            public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                //  1 ，存数据 ：
                listState.add(value);
                //  2 ，取数据 ：
                Iterable<Integer> dataIterable = listState.get();
                Iterator<Integer> dataIterator = dataIterable.iterator();


                List<Integer> dataList = IteratorUtils.toList(dataIterator);
                //  2 ，打印 ：
                System.out.println("key：" + ctx.getCurrentKey() + "，值：" + dataList);
                out.collect(value);
            }
        });

        //  6 ，打印 ：
        dsRes.print();
        //  7 ，执行 ：
        env.execute();
    }

}
