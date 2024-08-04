package com.sfl.flink.source;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.ArrayList;
import java.util.List;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWs03 implements GeneratorFunction<Long,WaterSensor> {

    private List<WaterSensor> list;

    public void mkData(){
        list = new ArrayList<>();
        for (int i = 1; i < 1000; i++) {
            Integer valueI = i;
            this.list.add(new WaterSensor(i,valueI.longValue()*1000,i));
        }
        list.set(4,new WaterSensor(4,4997l,4));
        list.set(5,new WaterSensor(5,4998l,5));
        list.set(6,new WaterSensor(6,4999l,6));
        list.set(7,new WaterSensor(7,5000l,7));
        list.set(8,new WaterSensor(8,5001l,8));
        list.set(9,new WaterSensor(9,5002l,9));
        list.set(10,new WaterSensor(10,5003l,10));
        list.set(11,new WaterSensor(11,5004l,11));
        list.set(12,new WaterSensor(12,5005l,12));

    }

    @Override
    public WaterSensor map(Long value) throws Exception {
        mkData();
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(new Date());
//        System.out.println(calendar.get(Calendar.SECOND));
        return this.list.get(value.intValue());
    }
}
