package com.ji.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

// 流处理程序
public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    // 核心处理流程: 过滤日志信息，留下评分数据
    @Override
    public void process(byte[] key, byte[] value) {
        // byte[]方便传输，String方便处理
        String input = new String(value);
        // 提取数据，过滤日志信息 (规定评分日志信息以 PRODUCT_RATING_PREFIX: 开头)
        if (input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("rating data coming!!! : " + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            // 按拓扑结构发送出去
            context.forward("logProcessor".getBytes(),input.getBytes());
        }


    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
