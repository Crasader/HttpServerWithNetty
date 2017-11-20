package com.mao.netty;

import com.mao.netty.kafka.KafkaUserProducer;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;

public class HTTPServerHandler extends SimpleChannelInboundHandler<FullHttpRequest>{

    KafkaUserProducer kafkaUserProducer = new KafkaUserProducer();

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest msg) throws Exception {
        String json = msg.content().toString(StandardCharsets.UTF_8);
        System.out.println(json);
        boolean success = kafkaUserProducer.SendData(json);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, success ? HttpResponseStatus.OK : HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.EMPTY_BUFFER, false);
        channelHandlerContext.write(response).addListener(ChannelFutureListener.CLOSE);
        return;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
    }
