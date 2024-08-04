package com.sfl.flink.ds03State;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//  1 ，泛型 ：输入，输出
public class MyMapFunction extends RichMapFunction<Integer, String> implements CheckpointedFunction {
    //  1 ，本地变量 ：
    private List<Integer> list = new ArrayList<>();

    //  2 ，状态 ：列表状态【可以放到 keyed 流中使用，也可以放到 operator 流中使用】
    private ListState<Integer> listState;

    //  3 ，状态初始化 ：把数据，从状态中拿出来，交给本地变量
    //      调用时机 ：程序启动【或恢复】时，调用这个方法
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("initializeState...");
        //  3.1 从 上下文 初始化 算子状态
        listState = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Integer>("listState", Types.INT));

        //  3.2 从 算子状态中 把数据 拷贝到 本地变量【如果有快照的话】
        if (context.isRestored()) {
            //  获取迭代数据 ：
            Iterable<Integer> stateGetDataIterable = listState.get();
            Iterator<Integer> stateGetDataIterator = stateGetDataIterable.iterator();
            //  将数据，交给本地集合
            list = IteratorUtils.toList(stateGetDataIterator);
        }
    }

    //  4 ，本地变量持久化：【将 本地变量 拷贝到 算子状态中】，开启 checkpoint 时才会调用
    //      调用时机 ：检查点快照时，调用这个方法
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState...");
        // 3.1 清空算子状态
        listState.clear();
        // 3.2 将 本地变量 添加到 算子状态 中
        listState.update(list);
    }

    //  5 ，逻辑 ：
    @Override
    public String map(Integer value) throws Exception {
        list.add(value);
        //  1 ，获取 ：上下文
        RuntimeContext context = getRuntimeContext();
        //  2 ，获取 ：子任务编号
        int index = context.getIndexOfThisSubtask();
        if (index == 0) {
            System.out.println("---------------------------------------" + list);
            return "---------------------------------------" + value;
        } else if (index == 1) {
            System.out.println("=======================" + list);
            return "=======================" + value;
        } else {
            System.out.println("三三三" + list);
            return "三三三" + value;
        }
    }
}
