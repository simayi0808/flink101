package com.sfl.flink.source;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWaterSensor implements GeneratorFunction<Long,WaterSensor> {
    @Override
    public WaterSensor map(Long value) throws Exception {
        //  System.out.println(value);
        WaterSensor ws = new WaterSensor();
        ws.setId((int)(value%10));
        ws.setTs(value);
        ws.setVc(100 + (int)(Math.random()*10));
        return ws;
    }
}
