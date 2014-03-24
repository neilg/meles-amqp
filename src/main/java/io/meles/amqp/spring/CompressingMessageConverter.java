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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

public class CompressingMessageConverter extends AbstractMessageConverter {

    private final MessageConverter underlyingConverter;
    private final Compressor compressor;
    private final String encodingName;
    private final Pattern encodingPattern;


    public CompressingMessageConverter(final MessageConverter underlyingConverter, final Compressor compressor, final String encodingName) {

        if (underlyingConverter == null) {
            throw new IllegalArgumentException("underlyingConverter must not be null");
        }
        if (compressor == null) {
            throw new IllegalArgumentException("compressor must not be null");
        }
        if (encodingName == null) {
            throw new IllegalArgumentException("encodingName must not be null");
        }
        this.underlyingConverter = underlyingConverter;
        this.compressor = compressor;
        this.encodingName = encodingName;
        encodingPattern = Pattern.compile("^" + Pattern.quote(encodingName) + "(;(.*))?$");
    }

    @Override
    protected Message createMessage(final Object object, final MessageProperties messageProperties) {
        final Message messageToCompress = underlyingConverter.toMessage(object, messageProperties);
        final byte[] uncompressedBody = messageToCompress.getBody();

        final byte[] compressedBody = compressor.compress(uncompressedBody);

        if (compressedBody.length >= uncompressedBody.length) { // there's no benefit to compression
            return messageToCompress;
        }

        // TODO should we copy the properties
        final MessageProperties uncompressedProperties = messageToCompress.getMessageProperties();
        final String existingEncoding = uncompressedProperties.getContentEncoding();

        final String newEncodingName = existingEncoding == null || existingEncoding.trim().equals("") ?
                encodingName :
                encodingName + ";" + existingEncoding;

        uncompressedProperties.setContentEncoding(newEncodingName);

        return new Message(compressedBody, uncompressedProperties);
    }

    @Override
    public Object fromMessage(final Message message) throws MessageConversionException {
        final MessageProperties compressedProperties = message.getMessageProperties();

        final String contentEncoding = compressedProperties.getContentEncoding();
        final Matcher encodingMatcher = encodingPattern.matcher(contentEncoding);

        if (!encodingMatcher.matches()) {
            return underlyingConverter.fromMessage(message);
        }

        final byte[] compressedBody = message.getBody();
        final byte[] decompressedBody = compressor.decompress(compressedBody);

        // TODO should we copy the properties
        final String nextEncoding = encodingMatcher.group(2);
        compressedProperties.setContentEncoding(nextEncoding);
        return underlyingConverter.fromMessage(new Message(decompressedBody, compressedProperties));
    }

}
