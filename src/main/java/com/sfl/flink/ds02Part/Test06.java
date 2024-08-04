package com.sfl.flink.ds02Part;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Test06 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(2);

        //  3 ，数据源 ：
        DataStreamSource<String> dsSource01 = env.socketTextStream("cls01", 7777);
        DataStreamSource<String> dsSource02 = env.socketTextStream("cls01", 8888);

        //  4 ，合并 ：
        ConnectedStreams<String, String> dsConn = dsSource01.connect(dsSource02);

        //  5 ，分组 ：为了让两个流的数据，去【同一组中】相遇
        //      逻辑 ：字符串用【空格】分割，取第一个元素，作为 key
        ConnectedStreams<String, String> dsConnKeyed = dsConn.keyBy(e -> e.split(" ")[0], e -> e.split(" ")[0]);

        //  6 ，逻辑 ：关联，用到【状态】
        //      泛型 ：流一，流二，输出
        SingleOutputStreamOperator<String> dsRes = dsConnKeyed.process(new CoProcessFunction<String, String, String>() {
            //  1 ，定义 ：状态【k-v】
            private MapState<String, String> mapState;
            //  2 ，初始化状态 ：
            @Override
            public void open(Configuration parameters) throws Exception {
                //  初始化状态 ：
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,String>("mapState", Types.STRING,Types.STRING));
            }
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                //  获取 ：k
                String k = value.split(" ")[0];
                //  把值，放到状态中。
                mapState.put(k,value);
            }
            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                //  获取 ：k
                String k = value.split(" ")[0];
                //  从状态获取值 ：
                String s = mapState.get(k);
                //  逻辑 ：有值，用，没值，直接输出
                if(s!=null){
                    out.collect(k + "：" + s + "---------------" + value);
                }else{
                    out.collect(k + "： ---------------" + value);
                }
            }
        });

        //  5 ，打印 ：
        dsRes.print();

        //  6 ，执行 ：
        env.execute();
    }

}
