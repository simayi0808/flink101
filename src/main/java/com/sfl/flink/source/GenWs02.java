package com.sfl.flink.source;

import com.sfl.flink.bean.WaterSensor;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

//  泛型 ：自增的一个数字【1,2,3 ...】，输出类型
public class GenWs02 implements GeneratorFunction<Long,WaterSensor> {

    private List<WaterSensor> list;

    public void mkData(){
        list = new ArrayList<>();
        for (int i = 1; i < 1000; i++) {
            Integer valueI = i;
            this.list.add(new WaterSensor(i,valueI.longValue()*1000,i));
        }
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
