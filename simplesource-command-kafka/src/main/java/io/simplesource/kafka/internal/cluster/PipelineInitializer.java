package io.simplesource.kafka.internal.cluster;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;

public final class PipelineInitializer {

    private final MessageHandler messageHandler;

    public PipelineInitializer(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public void init(ChannelPipeline pipeline) {
        // decoders
        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
        pipeline.addLast("byteArrayDecoder", new ByteArrayDecoder());
        pipeline.addLast("commandResponseDecoder", Message.DECODER);
        // encoders
        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
        pipeline.addLast("byteArrayEncoder", new ByteArrayEncoder());
        pipeline.addLast("commandRequestEncoder", Message.ENCODER);
        // business logic handler
        pipeline.addLast("commandResponseHandler", messageHandler);
    }
}
