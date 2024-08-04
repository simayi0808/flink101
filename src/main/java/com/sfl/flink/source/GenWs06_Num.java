package com.sfl.flink.source;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.ArrayList;
import java.util.List;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWs06_Num implements GeneratorFunction<Long,Long> {
    @Override
    public Long map(Long value) throws Exception {
        return value;
    }
}
