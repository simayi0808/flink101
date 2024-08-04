package com.sfl.flink.ds03State;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class Test03 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：
        //      3.1，主体流 ：
        DataStreamSource<Integer> dsSource01 = env.fromElements(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15);
        //      3.2，广播流 ：
        DataStreamSource<Integer> dsSource02 = env.fromElements(1, 2, 3, 4, 5,6,7,8,9,10,11,12,13,14,15);

        //  4 ，分区 ：
        //      4.1，分开为 3 个并行度
        SingleOutputStreamOperator<Integer> dsMap02 = dsSource02.map(e -> e).setParallelism(3);
        DataStream<Integer> dsRep02 = dsMap02.partitionCustom(new Partitioner<Integer>() {
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

        //  5 ，广播 ：
        //      1 ，状态类型 ：选用 map
        MapStateDescriptor<String, List<Integer>> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.LIST(Types.INT));
        //      2 ，将流：广播
        BroadcastStream<Integer> dsBroad = dsRep02.broadcast(broadcastMapState);

        //  6 ，双流连接 ：
        BroadcastConnectedStream<Integer, Integer> dsConnect = dsSource01.connect(dsBroad);

        //  7 ，处理逻辑 ：
        //  SingleOutputStreamOperator<Integer> dsRes = dsConnect.process(new MyBroadProcessFunction(broadcastMapState));

        //  8 ，打印 ：
//        dsRep01.print().setParallelism(3);
//        dsRep02.print().setParallelism(3);
        //  dsRes.print().setParallelism(3);

        //  9 ，执行 ：
        env.execute();
    }

}
