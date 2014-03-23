package io.meles.amqp.spring;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.pholser.junit.quickcheck.ForAll;
import org.iq80.snappy.Snappy;
import org.junit.contrib.theories.Theories;
import org.junit.contrib.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class SnappyCompressorTest {

    @Theory
    public void decompressIsInverseOfCompress(@ForAll final byte[] bytes) {
        final SnappyCompressor compressorUnderTest = new SnappyCompressor();
        assertThat(
                compressorUnderTest.decompress(compressorUnderTest.compress(bytes)),
                is(equalTo(bytes)));
    }

    @Theory
    public void shouldProduceValidSnappyCompressedBytes(@ForAll final byte[] bytes) {
        final SnappyCompressor compressorUnderTest = new SnappyCompressor();

        final byte[] compressedBytes = compressorUnderTest.compress(bytes);

        // will fail if not valid
        Snappy.uncompress(compressedBytes, 0, compressedBytes.length);
    }

}
