package com.sfl.flink.ds01;

import com.alibaba.fastjson.JSONObject;
import com.sfl.flink.bean.WaterSensor;
import com.sfl.flink.source.GenWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test03 {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  2 ，设置并行度 ：
        env.setParallelism(1);

        //  3 ，数据源 ：数据生成器，生成几个数，生成速度【每秒2条】，数据类型，
        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<>(new GenWaterSensor(), Long.MAX_VALUE, RateLimiterStrategy.perSecond(2), Types.POJO(WaterSensor.class));

        //  4 ，读数据 ：源，水印策略，流名字
        DataStreamSource<WaterSensor> dsSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "waterSensor");

        //  5 ，过滤 ：
        SingleOutputStreamOperator<WaterSensor> dsRes = dsSource.filter(ws -> ws.getId() % 2 == 0);

        //  5 ，打印 ：
        dsRes.print();

        //  6 ，执行 ：
        env.execute();

    }

}
