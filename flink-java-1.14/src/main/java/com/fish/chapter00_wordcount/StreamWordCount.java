package com.fish.chapter00_wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yuyi-YuShaoyu
 * @Description
 * @create 2022-03-23 14:27
 * @Modified By
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        DataStreamSource<String> lineDS = env.readTextFile("data/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordDS = lineDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, collector) -> {
            for (String word : line.split(" ")) {
                collector.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> wordCountDS = wordDS
                .keyBy((KeySelector<Tuple2<String, Long>, String>) tuple -> tuple.f0)
                .sum(1);

        wordCountDS.print();

        //流环境需要execute才会触发计算
        env.execute();


        /**
         * 执行结果分析:
         * 5> (hello,1)
         * 9> (world,1)
         * 5> (hello,2)
         * 5> (hello,3)
         * 13> (flink,1)
         * 3> (java,1)
         *
         * System.out.println(env.getParallelism()); -> 默认并行度为 16
         * hello打印了三次说明 触发了三次sum?
         * hello都在5号线程上进行计算 说明keyBy后相同元素会被同一个线程/节点计算
         */

    }
}
