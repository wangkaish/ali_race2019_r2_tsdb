/*
 * Copyright 2015 The FireNio Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging;

import java.nio.ByteBuffer;

import static io.openmessaging.Unsafe.UNSAFE;
import static io.openmessaging.Unsafe.get_address;

/**
 * @author: wangkai
 **/
public class VLongBuf {

    long read_index;
    long write_index;
    long address;

    public long get_read_index() {
        return read_index - address;
    }

    public long get_write_index() {
        return write_index - address;
    }

    public boolean isReadable() {
        return read_index < write_index;
    }

    public long size() {
        return (write_index - address);
    }

    public void set(long address) {
        set(address, 0);
    }

    public void set(long address, long write_index) {
        this.address = address;
        this.read_index = address;
        this.write_index = address + write_index;
    }

    public void set_read_index(long read_index) {
        this.read_index = address + read_index;
    }

    public void clear_read() {
        this.read_index = address;
    }

    void put(byte b) {
        UNSAFE.putByte(write_index++, b);
    }

    byte get() {
        return UNSAFE.getByte(read_index++);
    }

    public long readVLong() {
        long b1  = get() & 0xff;
        long b2  = get() & 0xff;
        long b3  = get() & 0xff;
        long v   = 0;
        int  off = 0;
        for (; ; ) {
            long b = get();
            v |= (b & 0b0111_1111) << off;
            if (b >= 0) {
                break;
            }
            off += 7;
        }
        v <<= 24;
        return v | ((b1 << 16) | (b2 << 8) | (b3));
    }

    public void writeVLong(long v) {
        long w = v & (0xffffff);
        v >>>= 24;
        put((byte) (w >>> 16));
        put((byte) (w >>> 8));
        put((byte) (w));
        for (; ; ) {
            w = v & 0b0111_1111;
            v >>>= 7;
            if (v != 0) {
                put((byte) (w | 0b1000_0000));
            } else {
                put((byte) w);
                break;
            }
        }
    }

}
