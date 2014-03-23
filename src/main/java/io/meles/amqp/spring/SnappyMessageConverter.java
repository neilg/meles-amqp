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
