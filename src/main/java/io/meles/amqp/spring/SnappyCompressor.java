package io.meles.amqp.spring;

import org.iq80.snappy.Snappy;

public class SnappyCompressor {

    public byte[] compress(final byte[] uncompressedBody) {
        return Snappy.compress(uncompressedBody);
    }

    public byte[] decompress(final byte[] bytes) {
        return Snappy.uncompress(bytes, 0, bytes.length);
    }

}
