package com.sfl.flink.ds05Window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

//  开窗 ：key，时间，华东窗口
public class Test06WinKeyed {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //  2 ，源 ：
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，分组 ：奇偶数
        KeyedStream<String, Integer> dsKeyed = dsSource.keyBy(e -> Integer.parseInt(e) % 2);

        //  4 ，开窗 ：keyed，5s ，2s 滑动窗口
        WindowedStream<String, Integer, TimeWindow> dsWined = dsKeyed.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)));

        //  5 ，逻辑 ：
        //      泛型 ：输入，输出，窗口
        SingleOutputStreamOperator<String> dsRes = dsWined.process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<String> iterableElements, Collector<String> out) throws Exception {
                List<Integer> dataList = IteratorUtils.toList(iterableElements.iterator());
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
