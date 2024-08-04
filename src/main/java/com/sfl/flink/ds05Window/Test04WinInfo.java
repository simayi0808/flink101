package com.sfl.flink.ds05Window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalAggregateProcessAllWindowFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class Test04WinInfo {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //  2 ，源 ：
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，开窗 ：全局开窗，滚动，5s长度
        AllWindowedStream<String, TimeWindow> dsWindowed = dsSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));


        //  dsWindowed.aggregate(aggregate,process);

        //  dsWindowed.process(new InternalAggregateProcessAllWindowFunction<>());
        //  4 ，逻辑 ：
        //      泛型 ：输入，输出，窗口
        SingleOutputStreamOperator<String> dsRes = dsWindowed.process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<String> iterableElements, Collector<String> out) throws Exception {
                List<Integer> dataList = IteratorUtils.toList(iterableElements.iterator());
                out.collect(dataList.toString());
                TimeWindow window = context.window();

                long start = window.getStart();
                long end = window.getEnd();

                long maxTime = window.maxTimestamp();

                System.out.println("开始：" + start);
                System.out.println("结束：" + end);
                System.out.println("窗内最大：" + maxTime);
            }
        });

        //  8 ，打印 ：
        dsSource.print();
        dsRes.print();
        //  9 ，执行 ：
        env.execute();
    }

}
