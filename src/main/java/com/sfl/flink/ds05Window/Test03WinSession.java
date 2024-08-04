package com.sfl.flink.ds05Window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class Test03WinSession {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //  2 ，源 ：
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，开窗 ：全局开窗，会话，3s 不答话，就重新开窗
        AllWindowedStream<String, TimeWindow> dsSessionWin = dsSource.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(3)));

        //  4 ，处理 ：
        SingleOutputStreamOperator<String> dsRes = dsSessionWin.process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                List<Integer> dataList = IteratorUtils.toList(elements.iterator());
                out.collect(dataList.toString());
            }
        });

        //  8 ，打印 ：
        dsSource.print();
        dsRes.print();
        //  9 ，执行 ：
        env.execute();
    }

}
