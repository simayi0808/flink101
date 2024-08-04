package com.sfl.flink.test;

import com.sfl.flink.flat.SpaceFlat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_readFile {

    public static void main(String[] args) throws Exception {
        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  2 ，源 ：文件源
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("aa.txt")).build();
        //  3 ，读文件 ：不加时间水印，使用处理时间
        DataStreamSource<String> dsYuan = env.fromSource(source, WatermarkStrategy.noWatermarks(), "dataFromFile");
        //  4 ，一行转多行 ：
        SingleOutputStreamOperator<Tuple2<String, Long>> dsTupleTwo = dsYuan.flatMap(new SpaceFlat());
        //  5 ，分组 ：用第一个元素分组
        KeyedStream<Tuple2<String, Long>, String> dsKeyed = dsTupleTwo.keyBy(tp2 -> tp2.f0);
        //  6 ，组内求和 ：用第二个元素加到一起
        SingleOutputStreamOperator<Tuple2<String, Long>> dsRes = dsKeyed.sum(1);
        //  7 ，打印 ：
        dsRes.print().setParallelism(1);
        //  8 ，执行 ：
        env.execute();
    }

}
