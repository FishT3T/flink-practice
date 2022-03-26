package com.fish.chapter01_source;

import com.fish.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yuyi-YuShaoyu
 * @Description
 * @create 2022-03-26 17:03
 * @Modified By
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.readTextFile("data/clicks.txt");
        SingleOutputStreamOperator<Event> stream2 = stream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String line) throws Exception {
                String[] splits = line.split(",");
                return new Event(splits[0].trim(), splits[1].trim(), Long.parseLong(splits[2].trim()));
            }
        });
        stream2.print();
        env.execute();
    }
}
