package com.lenovo.kafkatohive.start;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.lenovo.kafkatohive.util.DateUtil;
import com.lenovo.kafkatohive.util.FileUtil;
import com.lenovo.kafkatohive.util.JDBCUtil;
import com.lenovo.kafkatohive.util.KafkaUtil;
import jodd.datetime.JDateTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hanjiang2 on 2019/7/30.
 */
public class KafkaToHive {
    private static List<String> topics;
    private static Map<String,Map<String,String>> topicTables = new HashMap<String,Map<String,String>>();
    //保存在本地的路径
    private static String parent_path = "D:\\项目\\kafka_data";
    //读取mysql，获取主题列表; 和 主题表字段映射
    static {
        topics = JDBCUtil.getAllTopic();
        System.out.println(topics);
        if(topics.size()>0){
            for(String topic : topics){
                System.out.println(topic);
                Map<String,String> map1 = JDBCUtil.getTopicTable(topic);
                System.out.println(JSON.toJSONString(map1));
                topicTables.put(topic,map1);
            }
        }
        //创建本地文件夹
        FileUtil.makeDir(topics, parent_path);
    }
    public static void main(String[] args) {
        new KafkaToHive().threadPoolConsumer();
    }

    public  void threadPoolConsumer(){
        if(topics.size()>0){
            int poolNum = topics.size();
            ExecutorService pool = Executors.newFixedThreadPool(poolNum);
            for(int i=0; i<poolNum; i++){
                pool.execute(new ConsumerKafka(KafkaUtil.getConsumerProp(),topics.get(i)));
            }
            // 关闭线程池
            pool.shutdown();
        }
    }
    //内部类
    class ConsumerKafka implements Runnable{
        KafkaConsumer<String,String> kafkaConsumer = null;
        Map<String,String> topicTable = null;
        String topic = null;
        public ConsumerKafka(Properties prop, String topic){
            prop.put("group.id",topic);
            prop.put("key.deserializer", StringDeserializer.class.getName());
            prop.put("value.deserializer", StringDeserializer.class.getName());
            kafkaConsumer = new KafkaConsumer<String, String>(prop);
            kafkaConsumer.subscribe(Arrays.asList(topic));
            this.topic = topic;
            System.out.println("正在消费的主题："+this.topic);
            topicTable = topicTables.get(topic);
        }
        @Override
        public void run() {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(2000);
            System.out.println("当前topic："+this.topic+",正在消费的数据的个数："+records.count());
            int field_num = topicTable.keySet().size();
            StringBuffer sb = new StringBuffer();
            for (ConsumerRecord<String, String> record : records) {
                JSONArray results = JSON.parseArray(record.value());
                for(Object object : results){
                    Map<String,String> result = (Map<String,String>) object;
                    for(int i =1 ; i<=field_num ; i++){
                        if(i != field_num)

                            sb.append(result.getOrDefault(topicTable.get("field"+i),"null")+",");
                        else
                            sb.append(result.getOrDefault(topicTable.get("field"+i),"null"));
                    }
                    sb.append("\n");
                }
                kafkaConsumer.commitSync();
            }
            String records_string = sb.toString();
//            String records_string = sb.substring(0,sb.length()-1);
            //获取当前日期
            String curr_time = DateUtil.getCurrTime();
            String curr_timestamp = DateUtil.getCurrTimeStamp();
            try {
                FileUtil.writeStringToLocal(parent_path+"\\"+this.topic+"_"+curr_time+"\\"+curr_timestamp+".txt",records_string);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}


