package com.mao.netty.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mao.protobuf.UserInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Map;
import java.util.Properties;

public class KafkaUserProducer {

    private KafkaProducer<Integer, UserInfo.User> producer ;
    int key = 0;


    public KafkaUserProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        producer = new KafkaProducer<>(props, new IntegerSerializer(), new UserProtobufSerializer());
    }

    public boolean SendData(String jsonData){
        try {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> jsonMap;
        jsonMap = objectMapper.readValue(jsonData,  new TypeReference<Map<String, String>>(){});

        UserInfo.User user = UserInfo.User.newBuilder().setName(jsonMap.get("name")).setAge(Long.parseLong(jsonMap.get("age"))).build();

            producer.send(new ProducerRecord<>("test",key++,user)).get();
            System.out.println("Done");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
