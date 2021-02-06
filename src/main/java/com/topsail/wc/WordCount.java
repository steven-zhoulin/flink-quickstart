package com.topsail.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理 Word Count
 *
 * @author Steven
 * @date 2021-02-06
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "D:\\idea-workspace\\flink-quickstart\\src\\main\\resources\\hello.txt";
        DataSource<String> dataSource = executionEnvironment.readTextFile(inputPath);
        DataSet<Tuple2<String, Long>> dataSet = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);
        dataSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Long>> {
        public void flatMap(String s, Collector<Tuple2<String, Long>> out) {
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Long>(word, 1L));
            }
        }
    }
}
