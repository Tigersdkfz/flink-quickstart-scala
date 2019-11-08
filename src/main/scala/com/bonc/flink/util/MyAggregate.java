package com.bonc.flink.util;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Description: 聚合类$
 * @Param: $params$
 * @return: $returns$
 * @Author: ciri
 * @Date: $date$
 */
public class MyAggregate {
    public static class AverageAggregate implements AggregateFunction{

        @Override
        public Object createAccumulator() {
            return null;
        }

        @Override
        public Object add(Object value ,Object accumulator) {
            return null;
        }

        @Override
        public Object getResult(Object accumulator) {
            return null;
        }

        @Override
        public Object merge(Object a ,Object b) {
            return null;
        }
    }
}
