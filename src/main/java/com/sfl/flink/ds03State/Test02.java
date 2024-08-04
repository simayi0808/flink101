package com.sfl.flink.ds03State;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Test02 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15);

        //  4 ，分区 ：
        //      4.1，分开为 3 个并行度
        SingleOutputStreamOperator<Integer> dsMap = dsSource01.map(e -> e).setParallelism(3);
        //      4.2，分派数据(自定义分区)，1-5,6-10,11-15
        DataStream<Integer> dsRep = dsMap.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer k, int numPartitions) {
                if (k <= 5) {
                    return 0;
                } else if (k > 5 && k <= 10) {
                    return 1;
                } else {
                    return 2;
                }
            }
        }, e -> e);

        //  5 ，子任务 ：查看每个子任务
        SingleOutputStreamOperator<String> dsRes = dsRep.map(new MyMapFunction()).setParallelism(3);

        //  5 ，打印 ：
        dsRes.print().setParallelism(3);

        //  6 ，执行 ：
        env.execute();
    }

}
