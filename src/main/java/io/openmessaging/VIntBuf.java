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
public abstract class VIntBuf {

    long read_index;
    long write_index;

    public int readZigZag() {
        int v = readVInt();
        return Util.zigZagDe(v);
    }

    public void writeZigZag(int v) {
        int w = Util.zigZagEn(v);
        writeVInt(w);
    }

    public void set_read_index(long read_index) {
        this.read_index = read_index;
    }

    public void set_write_index(long write_index) {
        this.write_index = write_index;
    }

    public long get_read_index() {
        return read_index;
    }

    public long get_write_index() {
        return write_index;
    }

    public boolean isReadable() {
        return get_read_index() < get_write_index();
    }

    public VIntBuf slice(long read_index, long write_index) {
        throw new UnsupportedOperationException();
    }

    void put(byte b) {
        throw new UnsupportedOperationException();
    }

    int get() {
        throw new UnsupportedOperationException();
    }

    public VIntBuf copy() {
        throw new UnsupportedOperationException();
    }


    public int readVInt() {
        int v   = 0;
        int off = 0;
        for (; isReadable(); ) {
            int b = get();
            v |= (b & 0b0111_1111) << off;
            if (b >= 0) {
                break;
            }
            off += 7;
        }
        return v;
    }

    public void writeVInt(int v) {
        for (; ; ) {
            int w = v & 0b0111_1111;
            v >>>= 7;
            if (v != 0) {
                put((byte) (w | 0b1000_0000));
            } else {
                put((byte) w);
                break;
            }
        }
    }

    public static class HeapVIntBuf extends VIntBuf {

        final byte[] memory;
        final int    direct_cap;
        int heap_read_index;
        int heap_write_index;

        HeapVIntBuf(byte[] memory, long read_index, long write_index, int direct_cap) {
            this.direct_cap = direct_cap;
            this.memory = memory;
            this.set_read_index(read_index);
            this.set_write_index(write_index);
        }

        @Override
        int get() {
            return memory[heap_read_index++];
        }

        @Override
        public void set_read_index(long read_index) {
            super.set_read_index(read_index);
            this.heap_read_index = (int) (read_index - direct_cap);
        }

        @Override
        public void set_write_index(long write_index) {
            super.set_write_index(write_index);
            this.heap_write_index = (int) (write_index - direct_cap);
        }

        @Override
        public long get_read_index() {
            return heap_read_index + direct_cap;
        }

        @Override
        public long get_write_index() {
            return heap_write_index + direct_cap;
        }

        @Override
        public String toString() {
            return "heap r: " + heap_read_index + ", w: " + heap_write_index;
        }
    }

    public static class DirectVIntBuf extends VIntBuf {

        final long direct_addr;

        DirectVIntBuf(long read_index, long write_index, long direct_addr) {
            this.direct_addr = direct_addr;
            this.set_read_index(read_index);
            this.set_write_index(write_index);
        }

        @Override
        int get() {
            return UNSAFE.getByte(direct_addr + read_index++);
        }

        @Override
        public String toString() {
            return "direct_memory r: " + read_index + ", w: " + write_index;
        }
    }

    public static class ComVIntBuf extends VIntBuf {

        final ByteBuffer         direct_memory;
        final long               capacity;
        final byte[]             heap_memory;
        final long               direct_address;
        final DirectVIntBuf      direct_slice;
        final HeapVIntBuf        heap_slice;
        final ReadOnlyComVIntBuf com_clice;

        ComVIntBuf(byte[] heap_memory, ByteBuffer direct_memory) {
            int direct_cap = direct_memory.capacity();
            this.heap_memory = heap_memory;
            this.direct_memory = direct_memory;
            this.direct_address = get_address(direct_memory);
            this.capacity = (heap_memory.length * 1L + direct_memory.capacity());
            this.direct_slice = new DirectVIntBuf(0, 0, direct_address);
            this.heap_slice = new HeapVIntBuf(this.heap_memory, direct_cap, direct_cap, direct_cap);
            this.com_clice = new ReadOnlyComVIntBuf(this.heap_memory, direct_address, direct_cap, 0, capacity);
        }

        ComVIntBuf(int heap, int direct) {
            this(new byte[heap], Util.allocateDirect(direct));
        }

        @Override
        void put(byte b) {
            long write_index = this.write_index++;
            if (write_index < direct_memory.capacity()) {
                UNSAFE.putByte(direct_address + write_index, b);
            } else {
                heap_memory[(int) (write_index - direct_memory.capacity())] = b;
            }
        }

        @Override
        public VIntBuf slice(long read_index, long write_index) {
            VIntBuf slice;
            if (read_index < direct_memory.capacity() && write_index < direct_memory.capacity()) {
                slice = this.direct_slice;
            } else if (read_index >= direct_memory.capacity() && write_index >= direct_memory.capacity()) {
                slice = this.heap_slice;
            } else {
                slice = com_clice;
            }
            slice.set_read_index(read_index);
            slice.set_write_index(write_index);
            return slice;
        }

        @Override
        public VIntBuf copy() {
            ComVIntBuf copy = new ComVIntBuf(this.heap_memory, this.direct_memory);
            copy.set_read_index(get_read_index());
            copy.set_write_index(get_write_index());
            return copy;
        }

        @Override
        public String toString() {
            return "com r: 0, w: " + write_index + ", cap: " + capacity;
        }
    }

    public static class ReadOnlyComVIntBuf extends VIntBuf {

        final byte[] heap_memory;
        final long   direct_memory;
        final int    direct_cap;

        ReadOnlyComVIntBuf(byte[] heap_memory, long direct_memory, int direct_cap, long read_index, long write_index) {
            this.heap_memory = heap_memory;
            this.direct_memory = direct_memory;
            this.direct_cap = direct_cap;
            this.set_read_index(read_index);
            this.set_write_index(write_index);
        }

        @Override
        int get() {
            long read_index = this.read_index++;
            if (read_index < direct_cap) {
                return UNSAFE.getByte(direct_memory + read_index);
            } else {
                return heap_memory[(int) (read_index - direct_cap)];
            }
        }

        @Override
        public String toString() {
            return "com(r) r: " + read_index + ", w: " + write_index;
        }

    }

}
