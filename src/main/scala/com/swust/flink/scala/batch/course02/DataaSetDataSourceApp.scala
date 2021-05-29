package com.swust.flink.scala.batch.course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @Author XueTong
 * @Slogan Your story may not have a such happy begining, but that doesn't make who you are. 
 *         It is the rest of your story, who you choose to be.
 * @Date 2021/5/29 18:25
 * @Function
 */
object DataaSetDataSourceApp {

  def main(args: Array[String]): Unit = {
    // 获取上下文环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    getSourceFormCollecion(environment)
  }

  def getSourceFormCollecion(environment: ExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val data =  1 to 10
    environment.fromCollection(data).print()
  }
}

