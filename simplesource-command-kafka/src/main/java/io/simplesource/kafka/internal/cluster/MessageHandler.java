package io.simplesource.kafka.internal.cluster;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.simplesource.api.CommandAPI;
import io.simplesource.api.CommandError;
import io.simplesource.data.*;
import io.simplesource.api.CommandError.Reason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessageHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private final CommandAPILoader aggregates;
    private final RequestResponseMapper requestResponseMapper;
    private final Client client;

    MessageHandler(Client client, CommandAPILoader aggregates, RequestResponseMapper requestResponseMapper) {
        this.aggregates = aggregates;
        this.requestResponseMapper = requestResponseMapper;
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) {
        msg.fold((request) -> {
            logger.trace("Got request:{}", request);
            CommandAPI<CommandError, NonEmptyList<Sequence>> commandAPI = getCommandAPI(request.aggregateName);
            if (commandAPI == null) {
                client.send(request.sourceHost, Message.response(request.requestId,
                        Result.failure(CommandError.of(Reason.RemoteLookupFailed,
                                "No Aggregate with name:" + request.aggregateName + " found"))));
            } else {
                FutureResult<CommandError, NonEmptyList<Sequence>> futureResult =
                        commandAPI.queryCommandResult(request.commandId, request.timeout);
                futureResult.future().thenApply((res) -> {
                    client.send(request.sourceHost, Message.response(request.requestId, res));
                    return null;
                });
            }
            return null;
        }, (response) -> {
            logger.trace("Got response:{}", response);
            requestResponseMapper.completeRequest(response);
            return null;
        });
    }

    @Override
    public boolean isSharable() {
        return true;
    }

    private CommandAPI<CommandError, NonEmptyList<Sequence>> getCommandAPI(String aggregateName) {
        return (CommandAPI<CommandError, NonEmptyList<Sequence>>) aggregates.get(aggregateName);
    }
}