package com.sfl.flink.source;

import org.apache.flink.connector.datagen.source.GeneratorFunction;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWs05_KeyGroup implements GeneratorFunction<Long,Long> {
    @Override
    public Long map(Long value) throws Exception {
        return value;
    }
}
