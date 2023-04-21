package com.ccy._00bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * 定义的WaterSensor，有这样几个特点：
 * （1）类是公有（public）的；
 * （2）有一个无参的构造方法；
 * （3）所有属性都是公有（public）的；
 * （4）所有属性的类型都是可以序列化的。
 * Flink会把这样的类作为一种特殊的POJO数据类型来对待，方便数据的解析和序列化。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}