package com.bonc.flink.stream

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
  * 演示广播变量
  *
  * @author wzq
  * @date 2019/9/1
  */
object BatchBroadCast {

  def main(args: Array[String]): Unit = {
    /*val environment = ExecutionEnvironment.getExecutionEnvironment
    val map = Map("zs" -> 1, "ls" -> 2, "ww" -> 3)
    import org.apache.flink.api.scala._
    val broadCastData = environment.fromCollection(map.toIterator)
    val data = environment.fromElements("zs", "ls", "ww")

    data
      //注意这里需要创建RichMapFunction匿名内部类，使用里面提供的open方法获取广播变量
      .map(new RichMapFunction[String, String] {

      var broadCast: mutable.Map[String, Int] = mutable.Map[String, Int]()

      /**
        * 该函数只会在当前算子执行之前被执行一次，比如map和join算子，因此可以在该函数中进行一些初始化操作。比如获取广播变量。
        *
        * @param parameters 配置参数对象，可以获取到所有的运行参数
        */
      override def open(parameters: Configuration): Unit = {
        val broadCastDataList: util.List[Tuple2[String, Int]] = getRuntimeContext.getBroadcastVariable[Tuple2[String, Int]]("broadCastData")
        //获取到的广播变量类型为list集合，集合泛型为具体广播的变量的泛型，处理时需要拿到集合的迭代器，然后通过迭代器处理里面的每一条数据
        val value: util.Iterator[Tuple2[String, Int]] = broadCastDataList.iterator()
        while (value.hasNext) {
          //val stringToInt: Map[String, Int] = value.next()
          val stringToInt: Tuple2[String, Int] = value.next()
          val keys = stringToInt.keySet
          keys.foreach(key => {
            broadCast.put(key, stringToInt.getOrElse(key, 0))
          })
          //.foreach(k => broadCast.put(k._1, k._2))
        }
        super.open(parameters)
      }

      /**
        * 该函数只会在当前算子执行完之后执行一次，比如map和join算子，一般用来执行一些资源释放操作
        */
      override def close(): Unit = {
        super.close()
      }

      /**
        * 具体的业务处理逻辑，
        *
        * @param value 处理到的每一条数据
        * @return 处理结果
        */
      override def map(value: String): String = {
        value + broadCast.getOrElse(value, "")
      }
    })
      //注意每个算子使用广播变量，都需要在算子后面添加广播变量，添加的广播变量只在前面的算子中生效。
      .withBroadcastSet(broadCastData, "broadCastData")
      .print()*/
  }

}
