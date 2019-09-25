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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import sun.nio.ch.DirectBuffer;

import static io.openmessaging.Util.log;
import static io.openmessaging.Util.printException;


/**
 * @author wangkai
 */
@SuppressWarnings("restriction")
public class Unsafe {

    static final sun.misc.Unsafe UNSAFE = getUnsafe();
    static final long            ARRAY_BASE_OFFSET;
    static final long            ADDRESS_BASE_OFFSET;

    static {
        try {
            ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            ADDRESS_BASE_OFFSET = UNSAFE.objectFieldOffset(Buffer.class.getDeclaredField("address"));
        } catch (NoSuchFieldException e) {
            printException(e);
            throw new Error(e);
        }
    }

    static void init() {
        log("unsafe init finished...");
    }

    static long get_address(ByteBuffer buffer) {
        //        return ((DirectBuffer) buffer).address();
        return UNSAFE.getLong(buffer, ADDRESS_BASE_OFFSET);
    }

    static long get_long(long address, long offset) {
        return UNSAFE.getLong(address + offset);
    }

    static int get_int(long address, long offset) {
        return UNSAFE.getInt(address + offset);
    }

    static void put_long(long address, long offset, long value) {
        UNSAFE.putLong(address + offset, value);
    }

    static void put_int(long address, long offset, int value) {
        UNSAFE.putInt(address + offset, value);
    }

    static void copy_data(long address, long offset, byte[] dst, int len) {
        UNSAFE.copyMemory(null, address + offset, dst, ARRAY_BASE_OFFSET, len);
    }

    static void copy_data(byte[] src, long address, long offset, int len) {
        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET, null, address + offset, len);
    }

    static void copy_data(long src, long dst, int len) {
        UNSAFE.copyMemory(src, dst, len);
    }

    static sun.misc.Unsafe getUnsafe() {
        sun.misc.Unsafe unsafe = null;
        try {
            unsafe = AccessController.doPrivileged(new PrivilegedExceptionAction<sun.misc.Unsafe>() {
                @Override
                public sun.misc.Unsafe run() throws Exception {
                    Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;

                    for (Field f : k.getDeclaredFields()) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x)) {
                            return k.cast(x);
                        }
                    }
                    return null;
                }
            });
        } catch (Throwable e) {
            printException(e);
            throw new Error("get unsafe failed", e);
        }
        return unsafe;
    }


}
