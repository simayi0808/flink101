package com.sfl.flink.test;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;


public class Test03_readList {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，集合 ：
        ArrayList<WaterSensor> list = new ArrayList<WaterSensor>();
        list.add(new WaterSensor(1,10000l,91));
        list.add(new WaterSensor(2,20000l,92));
        list.add(new WaterSensor(3,30000l,93));
        list.add(new WaterSensor(4,40000l,94));

        //  3 ，读数据 ：
        DataStreamSource<WaterSensor> dsSource = env.fromCollection(list);

        //  4 ，打印 ：
        dsSource.print();

        //  6 ，执行 ：
        env.execute();
    }
}