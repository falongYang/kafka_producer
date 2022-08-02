package com.eoi;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashMap;
import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) {
        String host = "192.168.162.1:9092";
        String topic = "flink-source-s1";


        Properties props = new Properties();
        // 必须
        props.put("bootstrap.servers",host);
        // 被发送到broker的任何消息的格式都必须是字节数组
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // 非必须参数配置
        // acks=0表明producer完全不管发送结果；
        // acks=all或-1表明producer会等待ISR所有节点均写入后的响应结果；
        // acks=1，表明producer会等待leader写入后的响应结果
        props.put("acks","-1");
        // 发生可重试异常时的重试次数
        props.put("retries",3);
        // producer会将发往同一分区的多条消息封装进一个batch中，
        // 当batch满了的时候，发送其中的所有消息,不过并不总是等待batch满了才发送消息；
        props.put("batch.size",323840);
        // 控制消息发送延时，默认为0，即立即发送，无需关心batch是否已被填满。
        props.put("linger.ms",10);
        // 指定了producer用于缓存消息的缓冲区大小，单位字节，默认32MB
        // producer启动时会首先创建一块内存缓冲区用于保存待发送的消息，然后由另一个专属线程负责从缓冲区中读取消息执行真正的发送
        props.put("buffer.memory",33554432);
        // 设置producer能发送的最大消息大小
        props.put("max.request.size",10485760);
        // 设置是否压缩消息，默认none
        props.put("compression.type","lz4");
        // 设置消息发送后，等待响应的最大时间
        props.put("request.timeout.ms",30);

        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        JSONObject msgJson = new JSONObject(new LinkedHashMap<>());
        for(int i = 1;i<100;i++){
            msgJson.put("id",i);
            msgJson.put("name",RandomNameUtils.fullName());
            msgJson.put("age",(int) (1 + Math.random() * (90 - 1 + 1)));
            producer.send(new ProducerRecord<>(topic,"key-"+i,msgJson.toString()));
            System.out.println("生成了" + i + "条消息：" + msgJson.toString());
        }
        producer.close();

    }
}
