package com.fish.chapter00_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Yuyi-YuShaoyu
 * @Description
 * @create 2022-03-23 12:01
 * @Modified By
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1. 获取Batch执行环境(DataSet API)
        // Flink 1.12 之后 DataSet API 处于软弃用的状态 -> 都用流环境：执行时加上 -Dexecution.runtime-mode=BATCH
        // 注意是java包下的Environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 读取Text文件 (项目相对路径从项目根目录开始)
        DataSource<String> lineDS = env.readTextFile("data/words.txt");
        //3. 按单词切分
//        FlatMapOperator<String, Tuple2<String, Long>> wordDS = flatMapMethod1(lineDS);
        FlatMapOperator<String, Tuple2<String, Long>> wordDS = flatMapMethod2(lineDS);

        //4. 按单词分组
        UnsortedGrouping<Tuple2<String, Long>> groupedWordDS = wordDS.groupBy(0);

        //5. 按单词数sum聚合
        AggregateOperator<Tuple2<String, Long>> countDS = groupedWordDS.sum(1);

        //6. 打印结果
        countDS.print();
    }

    private static FlatMapOperator<String, Tuple2<String, Long>> flatMapMethod2(DataSource<String> lineDS) {
        //Lambda表达式的方式： 1.需要显示生命函数类型 2.需要说明返回值类型 否则会因Collector泛型擦除报错
        return lineDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, collector) -> {
                for (String word : line.split(" ")) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }).returns(Types.TUPLE(Types.STRING,Types.LONG));
    }

    private static FlatMapOperator<String, Tuple2<String, Long>> flatMapMethod1(DataSource<String> lineDS) {
        return lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                    for (String word : line.split(" ")) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }
            });
    }
}
