package com.sfl.flink.source;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.ArrayList;
import java.util.List;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWs04 implements GeneratorFunction<Long,WaterSensor> {

    private List<WaterSensor> list;

    public void mkData(){
        list = new ArrayList<>();

        list.add(new WaterSensor(1,1000l,4));
        list.add(new WaterSensor(2,2000l,5));
        list.add(new WaterSensor(3,3000l,6));
        list.add(new WaterSensor(4,4000l,7));
        list.add(new WaterSensor(5,5000l,8));
        list.add(new WaterSensor(7,6000l,9));
        list.add(new WaterSensor(9,7000l,10));
        list.add(new WaterSensor(11,8000l,11));
        list.add(new WaterSensor(13,9000l,12));
        list.add(new WaterSensor(15,10000l,12));
        list.add(new WaterSensor(17,11000l,12));

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
