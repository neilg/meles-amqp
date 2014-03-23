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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import com.pholser.junit.quickcheck.ForAll;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.theories.Theories;
import org.junit.contrib.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConverter;

@RunWith(Theories.class)
public class SnappyMessageConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MessageConverter converter;
    private SnappyCompressor compressor;
    private SnappyMessageConverter snappyMessageConverterUnderTest;

    @Before
    public void setup() {
        converter = mock(MessageConverter.class);
        compressor = mock(SnappyCompressor.class);
        snappyMessageConverterUnderTest = new SnappyMessageConverter(converter, compressor);
    }

    @Test
    public void underlyingConverterIsRequired() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("underlyingConverter");

        new SnappyMessageConverter(null, compressor);
    }

    @Test
    public void compressorIsRequired() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("compressor");

        new SnappyMessageConverter(converter, null);
    }

    @Test
    public void shouldUseOriginalUncompressableMessage() {
        final byte[] underlyingBytes = {1, 2, 3, 4, 5, 6};
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(underlyingBytes, properties));
        when(compressor.compress(underlyingBytes)).thenReturn(underlyingBytes);

        final Message message = snappyMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getBody(), equalTo(underlyingBytes));
    }

    @Theory
    public void messageSizeIsNeverLarger(@ForAll final byte[] messageBytes) {
        final SnappyMessageConverter x = new SnappyMessageConverter(converter, new SnappyCompressor());
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(messageBytes, properties));

        final Message message = x.createMessage("Hello", properties);

        assertThat(message.getBody().length, lessThanOrEqualTo(messageBytes.length));
    }

    @Theory
    public void nonEmptyMessageContainsData(@ForAll final byte[] messageBytes) {
        assumeThat(messageBytes.length, not(0));

        final SnappyMessageConverter x = new SnappyMessageConverter(converter, new SnappyCompressor());
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(messageBytes, properties));

        final Message message = x.createMessage("Hello", properties);

        assertThat(message.getBody().length, not(0));

    }

    @Test
    public void shouldSetEncodingForCompressableMessage() {
        final byte[] underlyingBytes = {1, 2, 3, 4, 5, 6};
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(underlyingBytes, properties));
        when(compressor.compress(underlyingBytes)).thenReturn(new byte[]{1, 2});

        final Message message = snappyMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getMessageProperties().getContentEncoding(), equalTo("snappy"));
    }

    @Test
    public void shouldDecompressCompressedMessage() {
        final byte[] compressedBody = {1, 3, 5, 3, 3,};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding("snappy");

        when(compressor.decompress(compressedBody)).thenReturn(new byte[]{6, 4, 5, 6, 4, 32, 45, 62, 1});
        final Object resultBody = new Object();
        when(converter.fromMessage(any(Message.class))).thenReturn(resultBody);

        final Object convertedBody = snappyMessageConverterUnderTest.fromMessage(new Message(compressedBody, properties));

        assertThat(convertedBody, is(sameInstance(resultBody)));
    }

    @Test
    public void shouldOnlyDecompressSnappyEncoding() {
        final byte[] compressedBody = {1, 3, 5, 3, 3,};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding("this_is_not_snappy");

        final Object resultBody = new Object();
        when(converter.fromMessage(any(Message.class))).thenReturn(resultBody);

        final Object convertedBody = snappyMessageConverterUnderTest.fromMessage(new Message(compressedBody, properties));

        verifyNoMoreInteractions(compressor);

        assertThat(convertedBody, is(sameInstance(resultBody)));
    }
}
