package com.bonc.flink.stream

import java.util.Properties

import com.bonc.flink.util.MyFKStream
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.tuple.{Tuple, Tuple2}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleDeserializers
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.{AbstractDeserializationSchema, JSONKeyValueDeserializationSchema, KeyedDeserializationSchemaWrapper, TypeInformationKeyValueSerializationSchema}
import org.apache.kafka.clients.producer.Partitioner


/**
  * @Description: 消费kafka数据的测试类
  * @Param: $params$
  * @return: $returns$
  * @Author: ciri
  * @Date: $date$
  */
object TestKafkaStream {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "zb6:9092,zb5:9092,zb3:9092")
    properties.setProperty("zookeeper.connect", "zb1:2181")
    properties.setProperty("group.id", "mwx_group")
    //    properties.setProperty("flink.partition-discovery.interval-millis", PropertyUtil.getProperty("kafka.partition-discovery.interval-millis"))

    // 添加数据源,可以用simpleStringSchema
    //以下是用json的消费模式，看起来不是json的haul会报错
    //val text = environment.addSource(new FlinkKafkaConsumer[ObjectNode]("flumeCount", new JSONKeyValueDeserializationSchema(true), properties).setStartFromLatest())

    //val source:DataStream[Tuple2[String,String]] = environment.addSource(new FlinkKafkaConsumer("flumeCount",new TypeInformationKeyValueSerializationSchema[String,String](classOf[String],classOf[String],environment.getConfig),properties))
    //这里注意scala和java的tuple2，并不一样，要用啥都用啥，2019.10.29
    val sourceData: DataStream[Tuple2[String,String]] = environment.addSource(new FlinkKafkaConsumer[Tuple2[String,String]]("flumeCount",new MyFKStream,properties))
    //text.filter(s=>s.contains(""))
    // 对数据源进行过滤
    //    text.addSink(fun = > {
    //      println(text)
    //    })
    // 设置执行并行度
    val keyedStream = sourceData.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .reduce((t1,t2)=>new Tuple2[String,String](t1.f0,t1.f1+t2.f1))
      .print()

    // 设置数据持久的类
    //text.print()
    // execute program
    environment.execute("FlinkJob")

  }
}
