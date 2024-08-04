package com.sfl.flink.ds03State;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class Test04 {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        //  2 ，源 ：nc 源
        DataStreamSource<String> dsSource01 = env.socketTextStream("cls01", 7777);
        DataStreamSource<String> dsSource02 = env.socketTextStream("cls01", 8888);
        //  3 ，将 2 广播 ：
        //      1 ，状态类型 ：选用 map
        MapStateDescriptor<String, List<String>> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.LIST(Types.STRING));
        //      2 ，将流：广播
        BroadcastStream<String> broadcast = dsSource02.broadcast(broadcastMapState);
        //  4 ，连接 ：
        BroadcastConnectedStream<String, String> dsConn = dsSource01.connect(broadcast);
        //  5 ，处理 ：
        SingleOutputStreamOperator<String> dsRse = dsConn.process(new MyBroadProcessFunction(broadcastMapState));

        //  6 ，打印 ：
        dsRse.print();
        //  7 ，执行 ：
        env.execute();
    }

}
