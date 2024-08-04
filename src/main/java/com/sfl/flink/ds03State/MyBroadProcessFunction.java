package com.sfl.flink.ds03State;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

//  1 ，泛型 ：
//      一流输入类型，二流输入类型，输出类型
public class MyBroadProcessFunction extends BroadcastProcessFunction<String, String, String> {
    //  1 ，广播状态描述器 ：
    private MapStateDescriptor<String, List<String>> broadcastMapState;

    //  1 ，构造方法 ：将广播流的状态传入进来
    public MyBroadProcessFunction(MapStateDescriptor<String, List<String>> broadcastMapState){
        this.broadcastMapState = broadcastMapState;
    }

    //  2 ，处理数据 ：
    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        //  1 ，获取数据 ：只读
        ReadOnlyBroadcastState<String, List<String>> broadcastState = ctx.getBroadcastState(broadcastMapState);
        List<String> thresholdList = broadcastState.get("threshold");
        //  返回值 ：
        out.collect(value+"，广播数据获取："+thresholdList);
    }

    //  3 ，处理广播 ：
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
        //  1 ，获取状态 ：
        BroadcastState<String, List<String>> mapSate = ctx.getBroadcastState(broadcastMapState);
        //  2 ，获取数据 ：
        List<String> listLocal = mapSate.get("threshold");
        //  3 ，塞数据 ：
        if(listLocal==null){
            listLocal = new ArrayList<>();
            listLocal.add(value);
            mapSate.put("threshold",listLocal);
        }else{
            listLocal.add(value);
        }
        //  System.out.println(listLocal);
    }

    //  2 ，处理流 ：只能读取广播数据
//    @Override
//    public void processElement(Integer value, ReadOnlyContext ctx, Collector<Integer> out) throws Exception {
//        //  1 ，获取数据 ：只读
//        ReadOnlyBroadcastState<String, List<Integer>> broadcastState = ctx.getBroadcastState(broadcastMapState);
//        List<Integer> thresholdList = broadcastState.get("threshold");
//        System.out.println(thresholdList);
//        //  System.out.println("数据："+value);
//        //  返回值 ：
//        out.collect(value);
//    }
//
//    //  广播流 ：只有广播流，可以修改数据
//    @Override
//    public void processBroadcastElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
//        //  1 ，获取状态 ：
//        BroadcastState<String, List<Integer>> broadcastState = ctx.getBroadcastState(broadcastMapState);
//        //  2 ，获取数据 ：
//        List<Integer> thresholdList = broadcastState.get("threshold");
//        //  3 ，更新数据 ：
//        if(thresholdList==null){
//            List<Integer> list = new ArrayList();
//            list.add(value);
//            broadcastState.put("threshold", list);
//        }else{
//            thresholdList.add(value);
//        }
//        //  System.out.println("广播："+value);
//    }


}
