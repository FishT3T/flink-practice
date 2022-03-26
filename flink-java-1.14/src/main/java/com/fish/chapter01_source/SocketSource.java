package com.fish.chapter01_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yuyi-YuShaoyu
 * @Description
 * @create 2022-03-26 17:15
 * @Modified By
 */
public class SocketSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socket = env.socketTextStream("localhost", 7777);
        socket.print();
        env.execute();
    }
}
