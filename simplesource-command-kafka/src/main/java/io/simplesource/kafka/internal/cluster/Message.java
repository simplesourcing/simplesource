package io.simplesource.kafka.internal.cluster;

import io.simplesource.api.CommandAPI;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Reason;
import io.simplesource.data.Result;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.streams.state.HostInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static io.simplesource.kafka.internal.cluster.IOUtil.*;

public abstract class Message {

    public static MessageToMessageEncoder<Message> ENCODER = new MessageToMessageEncoder<Message>() {

        @Override
        protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) {
            out.add(msg.toByteArray());
        }

        @Override
        public boolean isSharable() {
            return true;
        }
    };

    public static MessageToMessageDecoder<byte[]> DECODER = new MessageToMessageDecoder<byte[]>() {

        @Override
        protected void decode(ChannelHandlerContext ctx, byte[] msg, List<Object> out) {
            out.add(Message.fromByteArray(msg));
        }

        @Override
        public boolean isSharable() {
            return true;
        }
    };


    public static CommandRequest request(long requestId, HostInfo sourceHost, String aggregateName, UUID commandId, Duration duration) {
        return new CommandRequest(requestId, sourceHost, aggregateName, commandId, duration);
    }

    public static Message fromByteArray(byte[] bytes) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        return ignoreIOException(() -> {
            int type = in.readInt();

            if (CommandRequest.TYPE == type) {
                return CommandRequest.read(in);
            }

            if (CommandResponse.TYPE == type) {
                return CommandResponse.read(in);
            }

            throw new IllegalStateException("Unknown message type");
        });

    }

    public static CommandResponse response(long requestId, Result<CommandAPI.CommandError, NonEmptyList<Sequence>> result) {
        return new CommandResponse(requestId, result);
    }


    private Message() {
    }

    public abstract boolean isRequest();

    public abstract int getTypeId();

    public abstract void write(DataOutputStream out);


    public <T> T fold(final Function<CommandRequest, T> f, final Function<CommandResponse, T> s) {
        return isRequest() ?
                f.apply((CommandRequest) this) :
                s.apply((CommandResponse) this);
    }


    public byte[] toByteArray() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        DataOutputStream out = new DataOutputStream(bytes);
        ignoreIOException(() -> {
            out.writeInt(getTypeId());
            write(out);
            return null;
        });
        return bytes.toByteArray();
    }

    // -- implementations

    @Data
    @AllArgsConstructor
    static public final class CommandRequest extends Message {

        private static int TYPE = 0;

        long requestId;
        HostInfo sourceHost;
        String aggregateName;
        UUID commandId;
        Duration timeout;

        public void write(DataOutputStream out) {
            ignoreIOException(() -> {
                out.writeLong(requestId);
                writeString(out, sourceHost.host());
                out.writeInt(sourceHost.port());
                writeString(out, aggregateName);
                out.writeLong(commandId.getMostSignificantBits());
                out.writeLong(commandId.getLeastSignificantBits());
                out.writeLong(timeout.toMillis());
                return null;
            });
        }

        private static CommandRequest read(DataInputStream in) {
            return ignoreIOException(() -> {
                long requestId = in.readLong();
                HostInfo hostInfo = new HostInfo(readString(in), in.readInt());
                String aggregateName = readString(in);
                UUID commandId = new UUID(in.readLong(), in.readLong());
                Duration duration = Duration.ofMillis(in.readLong());
                return new CommandRequest(requestId, hostInfo, aggregateName, commandId, duration);
            });
        }

        @Override
        public boolean isRequest() {
            return true;
        }

        @Override
        public int getTypeId() {
            return TYPE;
        }
    }

    @Data
    @AllArgsConstructor
    static public final class CommandResponse extends Message {

        private static int TYPE = 1;

        long requestId;
        Result<CommandAPI.CommandError, NonEmptyList<Sequence>> result;

        public void write(DataOutputStream out) {
            ignoreIOException(() -> {
                out.writeLong(requestId);
                result.fold(
                        (errors) -> ignoreIOException(() -> {
                            out.writeShort((short) 0);
                            out.writeInt(errors.size());
                            for (Reason<CommandAPI.CommandError> error : errors) {
                                writeString(out, error.getMessage());
                                writeString(out, error.getError().name());
                            }
                            return null;
                        }),
                        (sequences) -> ignoreIOException(() -> {
                            out.writeShort((short) 1);
                            out.writeInt(sequences.size());
                            for (Sequence seq : sequences) {
                                out.writeLong(seq.getSeq());
                            }
                            return null;
                        }));
                return null;
            });
        }

        public static CommandResponse read(DataInputStream in) {
            return ignoreIOException(() -> {
                long requestId = in.readLong();
                boolean isSuccess = in.readShort() == 1;

                if (!isSuccess) {
                    int size = in.readInt();
                    List<Reason<CommandAPI.CommandError>> errors = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        String msg = readString(in);
                        CommandAPI.CommandError error = CommandAPI.CommandError.valueOf(readString(in));
                        errors.add(i, Reason.of(error, msg));
                    }
                    return new CommandResponse(requestId, Result.failure(NonEmptyList.fromList(errors)));
                } else {
                    int size = in.readInt();
                    List<Sequence> sequences = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        sequences.add(i, Sequence.position(in.readLong()));
                    }
                    return new CommandResponse(requestId, Result.success(NonEmptyList.fromList(sequences)));
                }
            });
        }

        @Override
        public boolean isRequest() {
            return false;
        }

        @Override
        public int getTypeId() {
            return TYPE;
        }
    }

}
