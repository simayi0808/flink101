package com.sfl.flink.ds05Window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class Test05WinGlobal {

    public static void main(String[] args) throws Exception {

        //  1 ，执行环境 ：流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //  2 ，源 ：
        DataStreamSource<String> dsSource = env.socketTextStream("cls01", 7777);

        //  3 ，开窗 ：全局开窗
        AllWindowedStream<String, GlobalWindow> dsWindowed = dsSource.windowAll(GlobalWindows.create());

        //  4 ，触发器 ：
        AllWindowedStream<String, GlobalWindow> dsWined = dsWindowed.trigger(new Trigger<String, GlobalWindow>() {
            //  1 ，触发条件 ：4 个元素
            private Integer cnt=0;
            //  1 ，调用时机 ：每个元素转入，都会调用
            //  2 ，返回 ：决定了，是否触发计算，发出结果
            @Override
            public TriggerResult onElement(String element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                cnt++;
                if(cnt>=4){
                    cnt=0;
                    //  计算窗口，并发出结果
                    return TriggerResult.FIRE_AND_PURGE;
                }else{
                    //  元素来了，啥也不做，直接走你
                    return TriggerResult.CONTINUE;
                }
            }
            @Override
            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                //  元素来了，啥也不做，直接走你
                return TriggerResult.CONTINUE;
            }
            @Override
            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                //  元素来了，啥也不做，直接走你
                return TriggerResult.CONTINUE;
            }
            @Override
            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

            }
        });
        //  逻辑 ：
        SingleOutputStreamOperator<String> dsRes = dsWined.process(new ProcessAllWindowFunction<String, String, GlobalWindow>() {
            @Override
            public void process(Context context, Iterable<String> itb, Collector<String> out) throws Exception {
                List<Integer> dataList = IteratorUtils.toList(itb.iterator());
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
