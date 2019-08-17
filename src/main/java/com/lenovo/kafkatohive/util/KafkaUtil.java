package com.lenovo.kafkatohive.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by hanjiang2 on 2019/7/15.
 */
public class KafkaUtil {
    private static KafkaProducer<String,String> kafkaProducer;
    public static KafkaProducer<String,String> init(){
        if(kafkaProducer == null){
            Properties props = new Properties();
            //broker地址
            props.put("bootstrap.servers", "172.18.66.88:6667,172.18.66.89:6667,172.18.66.90:6667");

            //0是不获取反馈(消息有可能传输失败)
            //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
            //-1 | all是所有in-sync replicas接受到消息时的反馈
            //消息可靠性语义
            props.put("acks", "all");
            //key的序列化类
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            //value的序列化类
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            //请求broker失败进行重试的次数，避免由于请求broker失败造成的消息重复
            props.put("retries", 0);
            //按批发送，每批的消息数量
            props.put("batch.size", 16384);
            //防止来不及发送，延迟一点点时间，使得能够批量发送消息
            props.put("linger.ms", 1);
            //缓冲大小，bytes
            props.put("buffer.memory", 33554432);
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            kafkaProducer = producer;
        }
            return kafkaProducer;
    }
    public static String sendToKafka(String topic, String message){
        init();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,message);
        String result = null;
        try {
//            kafkaProducer.send(producerRecord, new Callback() {
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e != null) {
//                        System.out.println("e = " + e.getMessage());
//                    } else {
//                        System.out.println("recordMetadata = " + recordMetadata.topic() + "" + recordMetadata.offset());
//                    }
//                }
//            });
            Future future = kafkaProducer.send(producerRecord);
            future.get();
        }catch(Exception e){
            e.printStackTrace();
            return "发送失败";
        }finally {
            kafkaProducer.flush();
            kafkaProducer.close();
            kafkaProducer = null;
        }
        return "发送成功";
    }

    public static Properties getConsumerProp(){
        return PropertiesUtil.getProperties("/consumer.properties");
    }

}
