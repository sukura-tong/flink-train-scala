package com.swust.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author XueTong
 * @Slogan Your story may not have a such happy begining, but that doesn't make who you are.
 * It is the rest of your story, who you choose to be.
 * @Date 2021/5/9 19:39
 * @Function
 */
public class BatchWordCountByJava {
    public static void main(String[] args) {
        String input = "./data/words";
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> textFile = environment.readTextFile(input);
        try {
            textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] words = line.split(" ");
                    for (String word : words){
                        if(word != null && word.length() > 0){
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                }
            }).groupBy(0).sum(1).print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
