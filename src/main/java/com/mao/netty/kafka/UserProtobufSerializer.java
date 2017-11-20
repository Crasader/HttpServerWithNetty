package com.mao.netty.kafka;

import com.mao.protobuf.UserInfo;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserProtobufSerializer implements Serializer<UserInfo.User>{
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, UserInfo.User user) {
        return user.toByteArray();
    }

    @Override
    public void close() {

    }
}
