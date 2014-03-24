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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipCompressor implements Compressor {

    @Override
    public byte[] compress(final byte[] bytes) {

        final ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream(bytes.length);
        try (final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(compressedBytes)) {
            gzipOutputStream.write(bytes);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        return compressedBytes.toByteArray();
    }

    @Override
    public byte[] decompress(final byte[] bytes) {

        final ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream();
        try (final GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
            final byte[] buffer = new byte[10240];
            for (int bytesRead = gzipInputStream.read(buffer);
                 bytesRead != -1;
                 bytesRead = gzipInputStream.read(buffer)) {

                decompressedBytes.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return decompressedBytes.toByteArray();
    }
}
