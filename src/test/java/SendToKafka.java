import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

/**
 * Created by hanjiang2 on 2019/8/2.
 */
public class SendToKafka {

    public static void main(String[] args) throws Exception {

//        String topic = "kafkatestdemo";
        String topic = "kafkademo";
        Properties props = new Properties();
        //broker地址
        props.put("bootstrap.servers", "192.168.219.128:6667");

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


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String[] name = {"韩强","沈沉","夏宝保","丁鼎立","薛泷朱"};
        String[] v = {"v1","v2","v3","v4","v5"};
        String[] sex = {"男","女"};
        int[] age = {18,25,24,28,12};
        String[] hobby = {"打乒乓球","打篮球","游泳","打羽毛球","玩游戏"};
        String[] sdv = {"sdv1","sdv2","sdv3","sdv4","sdv5"};
        String[] df = {"df1","df2","df3","df4","df5"};
        String[] cdwe = {"cdwe1","cdwe2","cdwe3","cdwe4","cdwe5"};
        String[] xef = {"xef1","xef2","xef3","xef4","xef5"};
        String[] sefvw= {"sefvw1","sefvw2","sefvw3","sefvw4","sefvw5"};
        System.out.println("hello");
        Random ran = new Random();
//
        for (int i = 1; i < 100000000; i++) {
            String info = "[{'name':'"+name[ran.nextInt(5)]+"','v':'"+v[ran.nextInt(5)]+"','sex':'"+sex[ran.nextInt(2)]+"','age':'"+age[ran.nextInt(5)]+"','hobby':'"+hobby[ran.nextInt(5)]+"','sdv':'"+sdv[ran.nextInt(5)]+"','df':'"+df[ran.nextInt(5)]+"','cdwe':'"+cdwe[ran.nextInt(5)]+"','xef':'"+xef[ran.nextInt(5)]+"','sefvw':'"+sefvw[ran.nextInt(5)]+"'}]";
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic,info);
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("e = " + e.getMessage());
                    } else {
                        System.out.println("recordMetadata = " + recordMetadata.topic() + "" + recordMetadata.offset());
                    }
                }
            });
            Thread.sleep(500);
        }
        kafkaProducer.close();
    }

}
