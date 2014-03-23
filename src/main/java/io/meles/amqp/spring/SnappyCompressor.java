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

import org.iq80.snappy.Snappy;

public class SnappyCompressor {

    public byte[] compress(final byte[] uncompressedBody) {
        return Snappy.compress(uncompressedBody);
    }

    public byte[] decompress(final byte[] bytes) {
        return Snappy.uncompress(bytes, 0, bytes.length);
    }

}
