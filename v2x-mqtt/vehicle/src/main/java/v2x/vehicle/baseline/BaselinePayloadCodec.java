package v2x.vehicle.baseline;

import java.nio.charset.StandardCharsets;

/**
 * Baseline TCP payload codec.
 *
 * payload = [8byte sendNano LE][4byte nameLen LE][name utf8][pcd bytes]
 */
public final class BaselinePayloadCodec {
    private BaselinePayloadCodec() {}

    public static final class Decoded {
        public final long sendNano;
        public final String fileName;
        public final int bodyOffset;
        public final int bodyLength;

        public Decoded(long sendNano, String fileName, int bodyOffset, int bodyLength) {
            this.sendNano = sendNano;
            this.fileName = fileName;
            this.bodyOffset = bodyOffset;
            this.bodyLength = bodyLength;
        }
    }

    public static byte[] encode(long sendNano, String fileName, byte[] body) {
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        int total = 8 + 4 + nameBytes.length + body.length;
        byte[] out = new byte[total];

        // sendNano LE
        out[0] = (byte) (sendNano);
        out[1] = (byte) (sendNano >>> 8);
        out[2] = (byte) (sendNano >>> 16);
        out[3] = (byte) (sendNano >>> 24);
        out[4] = (byte) (sendNano >>> 32);
        out[5] = (byte) (sendNano >>> 40);
        out[6] = (byte) (sendNano >>> 48);
        out[7] = (byte) (sendNano >>> 56);

        // nameLen LE
        int n = nameBytes.length;
        out[8]  = (byte) (n);
        out[9]  = (byte) (n >>> 8);
        out[10] = (byte) (n >>> 16);
        out[11] = (byte) (n >>> 24);

        System.arraycopy(nameBytes, 0, out, 12, nameBytes.length);
        System.arraycopy(body, 0, out, 12 + nameBytes.length, body.length);
        return out;
    }

    public static Decoded decode(byte[] payload) {
        if (payload == null || payload.length < 12) {
            throw new IllegalArgumentException("payload too short");
        }
        long sendNano = getLongLE(payload, 0);
        int nameLen = getIntLE(payload, 8);
        if (nameLen < 0 || 12 + nameLen > payload.length) {
            throw new IllegalArgumentException("invalid nameLen=" + nameLen + " payloadLen=" + payload.length);
        }
        String fileName = new String(payload, 12, nameLen, StandardCharsets.UTF_8);
        int bodyOffset = 12 + nameLen;
        int bodyLength = payload.length - bodyOffset;
        return new Decoded(sendNano, fileName, bodyOffset, bodyLength);
    }

    public static int getIntLE(byte[] a, int off) {
        return (a[off] & 0xff)
                | ((a[off + 1] & 0xff) << 8)
                | ((a[off + 2] & 0xff) << 16)
                | ((a[off + 3] & 0xff) << 24);
    }

    public static long getLongLE(byte[] a, int off) {
        return ((long) (a[off] & 0xff))
                | ((long) (a[off + 1] & 0xff) << 8)
                | ((long) (a[off + 2] & 0xff) << 16)
                | ((long) (a[off + 3] & 0xff) << 24)
                | ((long) (a[off + 4] & 0xff) << 32)
                | ((long) (a[off + 5] & 0xff) << 40)
                | ((long) (a[off + 6] & 0xff) << 48)
                | ((long) (a[off + 7] & 0xff) << 56);
    }
}
