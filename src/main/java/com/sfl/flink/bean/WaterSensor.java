package com.sfl.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    //  设备 id ：
    public Integer id;
    //  记录时间 ：
    public Long ts;
    //  水位高度 ：
    public Integer vc;
}