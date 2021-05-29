package com.swust.flink.scala.streaming.course01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author XueTong
 * @Slogan Your story may not have a such happy begining, but that doesn't make who you are. 
 *         It is the rest of your story, who you choose to be.
 * @Date 2021/5/10 9:21
 * @Function
 */
object StreamingWordCountByScala {
  def main(args: Array[String]): Unit = {
    val hostname = "47.108.28.217";
    var port = 9999;

    try {
      val tool: ParameterTool = ParameterTool.fromArgs(args)
      port = tool.getInt("port")
    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val metas: DataStream[String] = env.socketTextStream(hostname, port)

    import org.apache.flink.api.scala._
    metas.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .setParallelism(1)


    env.execute("Streaming APP Running")
  }
}
