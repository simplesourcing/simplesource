package io.simplesource.kafka.internal.cluster;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class IOUtil {

    public static void writeString(DataOutputStream out, String str) throws IOException {
        if (str == null) {
            out.writeInt(-1);
        } else {
            byte[] data = str.getBytes("UTF-8");
            out.writeInt(data.length);
            if (data.length > 0) out.write(data);
        }
    }

    public static String readString(DataInputStream in) throws IOException {
        // Read data
        int length = in.readInt();
        if (length == -1) return null;
        if (length == 0) return "";
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data, "UTF-8");
    }


    @FunctionalInterface
    public interface IgnoreIOException<O> {
        O run() throws IOException;
    }

    public static <O> O ignoreIOException(IgnoreIOException<O> runnable) {
        try {
            return runnable.run();
        } catch (IOException ioe) {
            throw new IllegalStateException("Unexpected IO exception");
        }
    }
}
