/*
 * Copyright (c) 2014 Neil Green
 *
 * This file is part of Meles AMQP.
 *
 * Meles AMQP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Meles AMQP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Meles AMQP.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.meles.amqp.spring;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.pholser.junit.quickcheck.ForAll;
import org.junit.contrib.theories.Theories;
import org.junit.contrib.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class GzipCompressorTest {

    @Theory
    public void decompressIsInverseOfCompress(@ForAll final byte[] bytes) {
        final GzipCompressor compressorUnderTest = new GzipCompressor();
        assertThat(
                compressorUnderTest.decompress(compressorUnderTest.compress(bytes)),
                is(equalTo(bytes)));
    }

//    @Theory
//    public void shouldProduceValidSnappyCompressedBytes(@ForAll final byte[] bytes) {
//        final SnappyCompressor compressorUnderTest = new SnappyCompressor();
//
//        final byte[] compressedBytes = compressorUnderTest.compress(bytes);
//
//        // will fail if not valid
//        Snappy.uncompress(compressedBytes, 0, compressedBytes.length);
//    }
}
