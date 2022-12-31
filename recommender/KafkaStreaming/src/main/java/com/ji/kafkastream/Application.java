package com.ji.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        // 定义输入和输出的topic
        String fromTopic = "log";
        String toTopic = "recommender";
        // 定义KafkaStream的配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,"hadoop102:2181");
        settings.put(StreamsConfig.STATE_DIR_CONFIG,"D:\\MyCodeProjects\\IDEA\\ECommerceRecommendSystem\\recommender\\KafkaStreaming\\src\\main\\java\\com\\ji\\kafkastream\\kafka-streams");

        // 创建KafkaStream配置对象
        StreamsConfig ksConfig = new StreamsConfig(settings);

        // 定义拓扑构建器 数据流流程的拓扑结构
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",fromTopic)
                .addProcessor("PROCESSOR",() -> new LogProcessor(),"SOURCE")    //过滤日志信息，留下评分数据
                .addSink("SINK",toTopic,"PROCESSOR");

        // 创建KafkaStream
        KafkaStreams streams = new KafkaStreams(builder, ksConfig);

        streams.start();
        System.out.println("kafkaStreams start>>>>>>>>>>>>>>>");

    }
}
