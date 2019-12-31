package com.bonc.flink.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

/**
 * @Description: 自定义kakfa的flink消费类$
 * @Param: $params$
 * @return: $returns$
 * @Author: ciri
 * @Date: $date$
 */
public class MyFKStream implements KeyedDeserializationSchema<Tuple2<String,String>> {
    @Override
    public Tuple2<String,String> deserialize(byte[] messageKey ,byte[] message ,String topic ,int partition ,long offset) throws IOException {
        Tuple2<String,String> tuple2 = new Tuple2();
        if (message!=null){
            //这里直接调用tostring会返回乱码，非我所欲
            tuple2.f1 = new String(message,"UTF-8");
        }
        if (messageKey!=null){tuple2.f0 = new String(messageKey,"UTF-8");}
        else {
            //int i = (int) (Math.random()*3);
            //tuple2.f0="kong"+i;
            //这里对根据逗号分隔的第一个字符进行赋K值，到Tuple的1上，若是按照整数值分为三个并行度，因为key只有三个
            //tuple2.f0=tuple2.f1.split(",")[0];//这个是适配子框的
            tuple2.f0=tuple2.f1;
        }
        return tuple2;

    }

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }


    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}
