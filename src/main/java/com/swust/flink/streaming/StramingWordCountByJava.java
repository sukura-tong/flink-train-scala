package com.swust.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author XueTong
 * @Slogan Your story may not have a such happy begining, but that doesn't make who you are.
 * It is the rest of your story, who you choose to be.
 * @Date 2021/5/10 8:55
 * @Function
 */
public class StramingWordCountByJava {



    public static void main(String[] args) {

        String hostname = "47.108.28.217";
        int port = 0;

        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");

        }catch (Exception e){
            port = 9999;
            System.out.println("端口未设置，使用默认9999");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream(hostname, port);

        stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    if (word != null && !"".equals(word)){
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();


        try {
            env.execute("execute flink streaming word count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
