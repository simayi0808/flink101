package com.sfl.flink.ds02Part;

import org.apache.flink.api.common.functions.Partitioner;

//  测试用 ：至少有 3 个分区
public class SflPertitioner implements Partitioner<Integer> {
    @Override
    public int partition(Integer key, int numPartitions) {
        if(key<4){
            return 0;
        }else if(key>=4 && key<8){
            return 1;
        }else{
            return 2;
        }
    }
}
