package com.swust.flink.scala.batch.course01

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @Author XueTong
 * @Slogan Your story may not have a such happy begining, but that doesn't make who you are. 
 *         It is the rest of your story, who you choose to be.
 * @Date 2021/5/9 19:32
 * @Function
 */
object BatchWordCountByScala {
  def main(args: Array[String]): Unit = {
    val input = "./data/words"
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val value: DataSet[String] = env.readTextFile(input)
    import org.apache.flink.api.scala._

    value.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
  }
}
