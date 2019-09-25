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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import sun.nio.ch.DirectBuffer;

/**
 * @author: wangkai
 **/
public final class Util {

    static final OpenOption[] FC_READ_OPS  = new OpenOption[]{StandardOpenOption.READ};
    static final OpenOption[] FC_WRITE_OPS = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};

    private static final String[] HEXES = new String[256];

    static {
        for (int i = 0; i < 16; i++) {
            HEXES[i] = "0" + Integer.toHexString(i);
        }
        for (int i = 16; i < HEXES.length; i++) {
            HEXES[i] = Integer.toHexString(i);
        }
    }

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    private static ThreadLocal threadLocal = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_FORMAT));

    public static DateFormat getDateFormat() {
        return (DateFormat) threadLocal.get();
    }

    public static String date() {
        return getDateFormat().format(new Date());
    }

    public static void log(String msg) {
        System.out.println(date() + ":::: " + msg);
    }

    static void release(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            ((DirectBuffer) buffer).cleaner().clean();
        }
    }

    public static Throwable trySetAccessible(AccessibleObject object) {
        try {
            object.setAccessible(true);
            return null;
        } catch (Exception e) {
            return e;
        }
    }

    public static long past(long start) {
        return System.currentTimeMillis() - start;
    }

    public static long past_nano(long start) {
        return System.nanoTime() - start;
    }

    static FileChannel open_read(File file) {
        try {
            return FileChannel.open(file.toPath(), FC_READ_OPS);
        } catch (IOException e) {
            printException(e);
            throw new Error(e);
        }
    }

    public static int zigZagDe(int v) {
        return (v >>> 1) ^ (-(v & 1));
    }

    public static int zigZagEn(int v) {
        return (v << 1) ^ (v >> 31);
    }

    static FileChannel open_write(File file) {
        try {
            return FileChannel.open(file.toPath(), FC_WRITE_OPS);
        } catch (IOException e) {
            printException(e);
            throw new Error(e);
        }
    }

    static void printException(Throwable e) {
        log(e.getMessage());
        e.printStackTrace(System.out);
    }

    public static String getHexString(byte[] array) {
        if (array == null || array.length == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder(array.length * 3 + 2);
        builder.append("[");
        for (int i = 0; i < array.length; i++) {
            builder.append(getHexString(array[i]));
            builder.append(" ");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }

    static ByteBuffer allocateDirect(int cap) {
        return ByteBuffer.allocateDirect(cap).order(ByteOrder.nativeOrder());
    }

    public static Field getDeclaredField(Class<?> clazz, String name) {
        if (clazz == null) {
            return null;
        }
        Field[] fs = clazz.getDeclaredFields();
        for (Field f : fs) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            printException(e);
        }
    }

    public static String getHexString(byte b) {
        return HEXES[b & 0xFF];
    }

    static void print_jvm_args() {
        MemoryMXBean memorymbean = ManagementFactory.getMemoryMXBean();
        log("堆内存信息: " + memorymbean.getHeapMemoryUsage());
        log("方法区内存信息: " + memorymbean.getNonHeapMemoryUsage());

        List<String> inputArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        log("\n#####################运行时设置的JVM参数#######################");
        log(String.valueOf(inputArgs));

        log("\n#####################运行时内存情况#######################");
        long totle = Runtime.getRuntime().totalMemory();
        log("总的内存量 [" + totle + "]");
        long free = Runtime.getRuntime().freeMemory();
        log("空闲的内存量 [" + free + "]");
        long max = Runtime.getRuntime().maxMemory();
        log("最大的内存量 [" + max + "]");
    }

}
