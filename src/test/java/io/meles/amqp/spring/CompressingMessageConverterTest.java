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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import com.pholser.junit.quickcheck.ForAll;
import org.hamcrest.Matchers;
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
public class CompressingMessageConverterTest {

    public static final String AN_ENCODING = "an_encoding";
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MessageConverter converter;
    private Compressor compressor;
    private CompressingMessageConverter compressingMessageConverterUnderTest;

    @Before
    public void setup() {
        converter = mock(MessageConverter.class);
        compressor = mock(Compressor.class);
        compressingMessageConverterUnderTest = new CompressingMessageConverter(converter, compressor, AN_ENCODING);
    }

    @Test
    public void underlyingConverterIsRequired() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("underlyingConverter");

        new CompressingMessageConverter(null, compressor, "someEncoding");
    }

    @Test
    public void compressorIsRequired() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("compressor");

        new CompressingMessageConverter(converter, null, "someEncoding");
    }

    @Test
    public void encodingNameIsRequired() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("encodingName");

        new CompressingMessageConverter(converter, compressor, null);
    }

    @Test
    public void shouldUseOriginalUncompressableMessage() {
        final byte[] underlyingBytes = {1, 2, 3, 4, 5, 6};
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(underlyingBytes, properties));
        when(compressor.compress(underlyingBytes)).thenReturn(underlyingBytes);

        final Message message = compressingMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getBody(), equalTo(underlyingBytes));
    }

    @Theory
    public void messageSizeIsNeverLarger(@ForAll final byte[] messageBytes, @ForAll final byte[] compressedBytes) {
        final MessageProperties properties = new MessageProperties();
        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(messageBytes, properties));
        when(compressor.compress(any(byte[].class))).thenReturn(compressedBytes);

        final Message message = compressingMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getBody().length, lessThanOrEqualTo(messageBytes.length));
    }

    @Test
    public void shouldSetEncodingForCompressableMessage() {
        final byte[] underlyingBytes = {1, 2, 3, 4, 5, 6};
        final MessageProperties properties = new MessageProperties();

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(underlyingBytes, properties));
        when(compressor.compress(underlyingBytes)).thenReturn(new byte[]{1, 2});

        final Message message = compressingMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getMessageProperties().getContentEncoding(), equalTo(AN_ENCODING));
    }

    @Test
    public void shouldDecompressCompressedMessage() {
        final byte[] compressedBody = {1, 3, 5, 3, 3,};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding(AN_ENCODING);

        when(compressor.decompress(compressedBody)).thenReturn(new byte[]{6, 4, 5, 6, 4, 32, 45, 62, 1});
        final Object resultBody = new Object();
        when(converter.fromMessage(any(Message.class))).thenReturn(resultBody);

        final Object convertedBody = compressingMessageConverterUnderTest.fromMessage(new Message(compressedBody, properties));

        verify(compressor).decompress(eq(compressedBody));
        assertThat(convertedBody, is(sameInstance(resultBody)));
    }

    @Test
    public void shouldOnlyDecompressSpecifiedEncoding() {
        final byte[] compressedBody = {1, 3, 5, 3, 3,};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding("this_is_not_an_encoding_we_expect");

        final Object resultBody = new Object();
        when(converter.fromMessage(any(Message.class))).thenReturn(resultBody);

        final Object convertedBody = compressingMessageConverterUnderTest.fromMessage(new Message(compressedBody, properties));

        verifyNoMoreInteractions(compressor);

        assertThat(convertedBody, is(sameInstance(resultBody)));
    }

    @Test
    public void shouldPrependEncoding() {
        final byte[] underlyingBytes = {1, 2, 3, 4, 5, 6};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding("underlying_enc");

        when(converter.toMessage(any(), eq(properties))).thenReturn(new Message(underlyingBytes, properties));
        when(compressor.compress(underlyingBytes)).thenReturn(new byte[]{1, 2});

        final Message message = compressingMessageConverterUnderTest.createMessage("Hello", properties);

        assertThat(message.getMessageProperties().getContentEncoding(), equalTo(AN_ENCODING + ";underlying_enc"));
    }

    @Test
    public void shouldPassOnUnderlyingEncoding() {
        final byte[] compressedBody = {1, 3, 5, 3, 3,};
        final MessageProperties properties = new MessageProperties();
        properties.setContentEncoding(AN_ENCODING + ";another_enc");

        final Object resultBody = new Object();
        when(converter.fromMessage(any(Message.class))).thenReturn(resultBody);

        final Object convertedBody = compressingMessageConverterUnderTest.fromMessage(new Message(compressedBody, properties));

        verify(converter).fromMessage(argThat(Matchers.<Message>hasProperty("messageProperties", hasProperty("contentEncoding", equalTo("another_enc")))));

        assertThat(convertedBody, is(sameInstance(resultBody)));
    }

}
