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

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

public class SnappyMessageConverter extends AbstractMessageConverter {

    private final MessageConverter underlyingConverter;
    private final SnappyCompressor compressor;

    public SnappyMessageConverter(final MessageConverter underlyingConverter, SnappyCompressor compressor) {
        if (underlyingConverter == null) {
            throw new IllegalArgumentException("underlyingConverter must not be null");
        }
        if (compressor == null) {
            throw new IllegalArgumentException("compressor must not be null");
        }
        this.underlyingConverter = underlyingConverter;
        this.compressor = compressor;
    }

    @Override
    protected Message createMessage(final Object object, final MessageProperties messageProperties) {
        final Message messageToCompress = underlyingConverter.toMessage(object, messageProperties);
        final byte[] uncompressedBody = messageToCompress.getBody();

        final byte[] compressedBody = compressor.compress(uncompressedBody);

        if (compressedBody.length >= uncompressedBody.length) { // there's no benefit to compression
            return messageToCompress;
        }

        final MessageProperties uncompressedProperties = messageToCompress.getMessageProperties();
        // TODO 1) should we copy the properties
        // TODO 2) what if there's already an encoding set
        uncompressedProperties.setContentEncoding("snappy");

        return new Message(compressedBody, uncompressedProperties);
    }

    @Override
    public Object fromMessage(final Message message) throws MessageConversionException {
        final MessageProperties compressedProperties = message.getMessageProperties();
        if (!"snappy".equals(compressedProperties.getContentEncoding())) {
            return underlyingConverter.fromMessage(message);
        }

        final byte[] compressedBody = message.getBody();
        final byte[] decompressedBody = compressor.decompress(compressedBody);

        // TODO should we copy the properties
        compressedProperties.setContentEncoding(null);
        return underlyingConverter.fromMessage(new Message(decompressedBody, compressedProperties));
    }

}
