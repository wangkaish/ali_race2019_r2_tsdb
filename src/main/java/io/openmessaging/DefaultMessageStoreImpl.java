package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.openmessaging.VIntBuf.ComVIntBuf;

import static io.openmessaging.Unsafe.*;
import static io.openmessaging.Util.*;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {

    static final String  FILE_PATH;
    static final boolean DEBUG_SORT          = false;
    static final boolean ONLINE              = Env.ONLINE;
    static final int     MSG_SIZE            = ONLINE ? 201600 * 10000 : 8400 * 10000;
    static final int     SEND_THREAD_NUM     = ONLINE ? 12 : 1;
    static final int     T_DATA_DIRECT       = ONLINE ? 1024 * 1024 * 1800 : 1024 * 1024 * 400;
    static final int     T_DATA_HEAP         = ONLINE ? 1024 * 1024 * 2046 : 1024 * 1024 * 400;
    static final int     A_LEN               = 8;
    static final int     A_BLOCK             = ONLINE ? 128 : 1024;
    static final int     B_LEN               = 34;
    static final int     T_LEN               = 8;
    static final int     T_A_RATE            = ONLINE ? 128 : 16;
    static final int     T_BLOCK             = A_BLOCK * T_A_RATE;
    static final int     T_BLOCK_MASK        = T_BLOCK - 1;
    static final int     A_BLOCK_MASK        = A_BLOCK - 1;
    static final int     MSG_SEND_CACHE_SIZE = 1024 * 512;
    static final int     BUFFER_SIZE         = 1024 * 64;
    static final int     BUFFER_SIZE_SIZE    = 128;
    static final int     SPARSE_INDEX_COUNT  = ONLINE ? MSG_SIZE / T_BLOCK : MSG_SIZE / T_BLOCK;
    static final int     COMPACT_INDEX_COUNT = ONLINE ? MSG_SIZE / A_BLOCK : MSG_SIZE / A_BLOCK;
    static final int     A_READ_BUFFER_SIZE  = 1024 * 512;
    static final int     B_READ_BUFFER_SIZE  = 1024 * 1024;

    static final ThreadLocal<MQueue>      L_M_QUEUE       = ThreadLocal.withInitial(MQueue::next_queue);
    static final ThreadLocal<MFileReader> L_M_FILE_READER = ThreadLocal.withInitial(MFileReader::next);

    static {
        try {
            if (ONLINE) {
                FILE_PATH = "/alidata1/race2019/data/";
            } else {
                FILE_PATH = "C://temp/race2019/";
            }
            File file = new File(FILE_PATH);
            if (!file.exists()) {
                file.mkdirs();
            }
            Unsafe.init();
            IOThread.init();
            MFile.init();
            MFileReader.init();
            MQueue.init();
        } catch (Exception e) {
            printException(e);
            throw new Error(e);
        }
    }

    volatile boolean write_complete;

    static ByteBuffer take_buffer() {
        ByteBuffer buffer = IOThread.BUFFERS.poll();
        if (buffer == null) {
            for (; ; ) {
                try {
                    buffer = IOThread.BUFFERS.poll(3000, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        continue;
                    }
                    return buffer;
                } catch (InterruptedException e) {
                    printException(e);
                }
            }
        }
        return buffer;
    }

    @Override
    public void put(Message message) {
        MQueue queue = L_M_QUEUE.get();
        MFile  file  = MFile.get();
        synchronized (file) {
            queue.add_message(message);
            file.put_m_queue(queue);
        }
    }

    void check_write_complete() {
        if (!write_complete) {
            synchronized (this) {
                if (write_complete) {
                    return;
                }
                try {
                    MFile.get().finish_write();
                } catch (Exception e) {
                    printException(e);
                    throw new Error(e);
                }
                write_complete = true;
            }
        }
    }

    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        check_write_complete();
        MFileReader reader = L_M_FILE_READER.get();
        try {
            return reader.get_message(aMin, aMax, tMin, tMax);
        } catch (Exception e) {
            printException(e);
            return null;
        }
    }

    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        MFileReader reader = L_M_FILE_READER.get();
        try {
            return reader.get_avg(aMin, aMax, tMin, tMax);
        } catch (IOException e) {
            printException(e);
            return -2;
        }
    }

    static class MFile {

        static final MFile M_FILE_INSTANCE = new MFile();

        final File        a_path_file;
        final File        b_path_file;
        final FileChannel a_write_channel;
        final FileChannel b_write_channel;
        final FileChannel a_read_channel;
        final FileChannel b_read_channel;
        final Message[]   sort_buf   = new Message[T_BLOCK];
        final VIntBuf     t_data_buf = new ComVIntBuf(T_DATA_HEAP, T_DATA_DIRECT);

        ByteBuffer index_t_sparse   = allocateDirect(SPARSE_INDEX_COUNT * 8);
        ByteBuffer index_t_data_pos = allocateDirect(COMPACT_INDEX_COUNT * 8);
        long[]     index_a_data_pos = new long[COMPACT_INDEX_COUNT];
        long[]     index_t_compact  = new long[COMPACT_INDEX_COUNT];
        long[]     index_a_compact  = new long[COMPACT_INDEX_COUNT];
        long[]     index_a_sum      = new long[COMPACT_INDEX_COUNT];
        long[]     index_a_min_max  = new long[COMPACT_INDEX_COUNT * 2];
        VLongBuf   a_data_buf_v     = new VLongBuf();
        ByteBuffer a_data_buf;
        ByteBuffer b_data_buf;
        MQueue     sort_head        = null;
        long       b_data_buf_addr;
        int        b_data_buf_pos;
        int        t_size;
        long       a_min;
        long       a_max;
        long       a_sum;
        int        sort_buf_size;
        int        index_a_compact_size;
        int        index_a_data_pos_size;
        int        index_t_compact_size;
        int        index_a_min_max_size;
        int        index_a_sum_size;
        long       t_inc_base;
        long       a_inc_base;
        long       a_file_size;

        boolean ready;
        int     add_size;

        MFile() {
            this.a_path_file = new File(FILE_PATH + "queue-data-a");
            this.b_path_file = new File(FILE_PATH + "queue-data-b");
            this.a_write_channel = Util.open_write(a_path_file);
            this.b_write_channel = Util.open_write(b_path_file);
            this.a_read_channel = open_read(a_path_file);
            this.b_read_channel = open_read(b_path_file);
            this.set_a_data_buf(newBuffer());
            this.set_b_data_buf(newBuffer());
            for (int i = 0; i < BUFFER_SIZE_SIZE; i++) {
                IOThread.BUFFERS.offer(newBuffer());
            }
        }

        static void init() {
        }

        static ByteBuffer newBuffer() {
            return allocateDirect(BUFFER_SIZE + B_LEN);
        }

        static MFile get() {
            return M_FILE_INSTANCE;
        }

        void set_a_data_buf(ByteBuffer a_data_buf) {
            this.a_data_buf = a_data_buf;
            this.a_data_buf_v.set(get_address(a_data_buf), a_data_buf.position());
        }

        void set_b_data_buf(ByteBuffer b_data_buf) {
            this.b_data_buf = b_data_buf;
            this.b_data_buf_pos = b_data_buf.position();
            this.b_data_buf_addr = get_address(b_data_buf);
        }

        void put_sparse_index(long t) {
            index_t_sparse.putLong(t);
        }

        void put_compact_index(long t, long t_pos, long a, long a_pos, long a_min, long a_max, long a_sum) {
            index_t_data_pos.putLong(t_pos);
            index_a_data_pos[index_a_data_pos_size++] = (a_pos);
            index_a_sum[index_a_sum_size++] = (a_sum);
            index_t_compact[index_t_compact_size++] = (t);
            index_a_compact[index_a_compact_size++] = (a);
            index_a_min_max[index_a_min_max_size++] = (a_min);
            index_a_min_max[index_a_min_max_size++] = (a_max);
        }

        void put_m_queue(MQueue queue) {
            if (!ready) {
                if (!queue.added) {
                    add_size++;
                    if (sort_head == null) {
                        sort_head = queue;
                    } else {
                        sort_head = insert_queue(sort_head, queue);
                    }
                    queue.added = true;
                }
                if (add_size == SEND_THREAD_NUM) {
                    ready = true;
                }
                return;
            }
            MQueue sort_head = this.sort_head;
            if (queue == sort_head) {
                for (; ; ) {
                    Message message = sort_head.remove();
                    if (message == null) {
                        break;
                    }
                    put_message(message);
                    MQueue next = sort_head.next;
                    if (next == null) {
                        break;
                    }
                    if (sort_head.cur_t() > next.cur_t()) {
                        sort_head = sort(next, sort_head);
                    }
                }
                this.sort_head = sort_head;
            }
        }

        static MQueue sort(MQueue head, MQueue unit) {
            MQueue curr = head;
            MQueue next = curr.next;
            for (; ; ) {
                if (next == null) {
                    curr.next = unit;
                    unit.next = null;
                    break;
                }
                if (unit.cur_t() > next.cur_t()) {
                    curr = next;
                    next = curr.next;
                } else {
                    curr.next = unit;
                    unit.next = next;
                    break;
                }
            }
            return head;
        }

        static MQueue insert_queue(MQueue head, MQueue unit) {
            if (unit.cur_t() > head.cur_t()) {
                return sort(head, unit);
            }
            unit.next = head;
            return unit;
        }

        void put_message(Message message) {
            Message[] sort_buf = this.sort_buf;
            sort_buf[sort_buf_size++] = message;
            if (sort_buf_size == T_BLOCK) {
                this.put_sparse_index(sort_buf[0].getT());
                QuickSort.sort(sort_buf, QuickSort.SORT_VALUE_A, 0, T_BLOCK - 1);
                for (int i = 0; i < T_BLOCK; i++) {
                    write_message(sort_buf[i]);
                }
                sort_buf_size = 0;
            }
        }

        void write_message(Message message) {
            VIntBuf    t_data_buf   = this.t_data_buf;
            ByteBuffer b_data_buf   = this.b_data_buf;
            VLongBuf   a_data_buf_v = this.a_data_buf_v;
            int        t_inc;
            long       a_inc;
            long       t            = message.getT();
            long       a            = message.getA();
            long       id_size      = this.t_size++;
            if ((id_size & A_BLOCK_MASK) == 0) {
                t_inc = 0;
                a_inc = 0;
                long t_pos = t_data_buf.get_write_index();
                long a_pos = a_file_size + a_data_buf_v.size();
                this.t_inc_base = t;
                this.a_inc_base = a;
                this.put_compact_index(t, t_pos, a, a_pos, a_min, a_max, a_sum);
                this.a_min = a;
                this.a_max = a;
                this.a_sum = a;
            } else {
                t_inc = (int) (t - t_inc_base);
                a_inc = a - a_inc_base;
                this.t_inc_base = t;
                this.a_inc_base = a;
                this.a_sum += a;
                this.a_max = a;
            }
            t_data_buf.writeZigZag(t_inc);
            copy_data(message.getBody(), b_data_buf_addr, b_data_buf_pos, B_LEN);
            b_data_buf_pos += B_LEN;
            if (b_data_buf_pos > BUFFER_SIZE) {
                b_data_buf.position(b_data_buf_pos);
                this.set_b_data_buf(IOThread.submit(this.b_write_channel, b_data_buf, BUFFER_SIZE, null));
            }
            a_data_buf_v.writeVLong(a_inc);
            if (a_data_buf_v.size() > BUFFER_SIZE) {
                this.a_data_buf.position((int) a_data_buf_v.size());
                this.a_file_size += BUFFER_SIZE;
                this.set_a_data_buf(IOThread.submit(this.a_write_channel, a_data_buf, BUFFER_SIZE, null));
            }
            if (DEBUG_SORT && t_size < 10_0000) {
                System.out.println("id: " + sort_buf_size + ", a: " + a + ", t: " + t);
            }
        }

        void finish_write() throws Exception {
            CountDownLatch latch     = new CountDownLatch(2);
            MQueue         sort_head = this.sort_head;
            for (; sort_head.has_next(); ) {
                put_message(sort_head.remove_force());
                if (sort_head.has_next_message_force()) {
                    if (sort_head.cur_t() > sort_head.next.cur_t()) {
                        sort_head = sort(sort_head.next, sort_head);
                    }
                } else {
                    MQueue temp = sort_head;
                    sort_head = sort_head.next;
                    temp.next = null;
                }
            }
            for (; sort_head.has_next_message_force(); ) {
                put_message(sort_head.remove_force());
            }
            this.put_sparse_index(sort_buf[0].getT());
            QuickSort.sort(sort_buf, QuickSort.SORT_VALUE_A, 0, sort_buf_size - 1);
            for (int i = 0; i < sort_buf_size; i++) {
                write_message(sort_buf[i]);
            }
            this.sort_buf_size = 0;
            Arrays.fill(sort_buf, null);
            this.index_a_min_max[index_a_min_max_size++] = (a_min);
            this.index_a_min_max[index_a_min_max_size++] = (a_max);
            this.index_a_sum[index_a_sum_size++] = (a_sum);
            this.a_data_buf.position((int) a_data_buf_v.size());
            this.b_data_buf.position(b_data_buf_pos);
            this.a_file_size += a_data_buf.position();
            this.index_a_data_pos[index_a_data_pos_size++] = a_file_size;
            this.set_a_data_buf(IOThread.submit(this.a_write_channel, a_data_buf, a_data_buf.position(), latch));
            this.set_b_data_buf(IOThread.submit(this.b_write_channel, b_data_buf, b_data_buf.position(), latch));
            MFileReader.sync_t_data_buf_write_index();
            latch.await();
        }

    }

    static class MFileReader {

        static final MFileReader   M_FILE_READERS[]   = new MFileReader[SEND_THREAD_NUM];
        static final AtomicInteger M_FILE_READERS_SEQ = new AtomicInteger();

        final int        index;
        final VIntBuf    t_data_buf;
        final VLongBuf   a_data_buf = new VLongBuf();
        final ByteBuffer a_read_buf = allocateDirect(A_READ_BUFFER_SIZE);
        final ByteBuffer b_read_buf = allocateDirect(B_READ_BUFFER_SIZE);

        static void init() {
            for (int i = 0; i < (ONLINE ? 12 : 1); i++) {
                M_FILE_READERS[i] = new MFileReader(i);
            }
        }

        static MFileReader next() {
            return M_FILE_READERS[M_FILE_READERS_SEQ.incrementAndGet() % SEND_THREAD_NUM];
        }

        static void sync_t_data_buf_write_index() {
            for (MFileReader r : M_FILE_READERS) {
                r.t_data_buf.set_write_index(MFile.get().t_data_buf.get_write_index());
            }
        }

        MFileReader(int index) {
            this.index = index;
            this.a_data_buf.set(get_address(a_read_buf));
            this.t_data_buf = MFile.get().t_data_buf.copy();
        }

        long get_avg(long min_a, long max_a, long min_t, long max_t) throws IOException {
            MFile       m_file                 = MFile.get();
            ByteBuffer  index_t_sparse         = m_file.index_t_sparse;
            ByteBuffer  index_t_data_pos       = m_file.index_t_data_pos;
            long[]      index_t_compact        = m_file.index_t_compact;
            long[]      index_a_min_max        = m_file.index_a_min_max;
            long[]      index_a_compact        = m_file.index_a_compact;
            long[]      index_a_data_pos       = m_file.index_a_data_pos;
            long[]      index_a_sum            = m_file.index_a_sum;
            FileChannel a_read_channel         = m_file.a_read_channel;
            VIntBuf     t_data_buf             = this.t_data_buf;
            VLongBuf    a_data_buf             = this.a_data_buf;
            ByteBuffer  a_read_buf             = this.a_read_buf;
            long        index_t_data_pos_addr  = get_address(index_t_data_pos);
            long        index_t_sparse_addr    = get_address(index_t_sparse);
            int         index_t_data_pos_limit = index_t_data_pos.position();
            int         t_size                 = m_file.t_size;
            int         t_index_cnt            = (index_t_sparse.position() >>> 3) - 1; // size -1
            int         t_index_min            = half_find(index_t_sparse_addr, 0, t_index_cnt, min_t);
            int         t_index_max            = get_t_index_max(index_t_sparse_addr, t_index_min, t_index_cnt, max_t);
            int         a_index_cnt            = (m_file.index_a_min_max_size >>> 1) - 1; // size -1
            int         a_index_min            = t_index_min * T_A_RATE;
            int         a_index_off            = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
            int         a_index_len            = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
            long        sum                    = 0;
            int         count                  = 0;
            if (a_index_len > 0) {
                int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                long a_read_pos = index_a_data_pos[a_index_off];
                int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + a_index_len + 1, a_index_cnt)] - a_read_pos);
                a_read_buf.clear().limit(a_read_len);
                do_read(a_read_channel, a_read_buf, a_read_pos);
                long    t             = index_t_compact[a_index_off];
                long    a             = index_a_compact[a_index_off];
                long    t_read_index  = get_long(index_t_data_pos_addr, a_index_off * 8);
                long    t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                VIntBuf t_data_temp   = t_data_buf.slice(t_read_index, t_write_index);
                a_data_buf.clear_read();
                for (int i = 0, part_i = 0; i < msg_size; i++) {
                    t += t_data_temp.readZigZag();
                    a += a_data_buf.readVLong();
                    if (t >= min_t && t <= max_t && a >= min_a && a <= max_a) {
                        sum += a;
                        count++;
                    }
                    part_i++;
                    if (part_i == A_BLOCK) {
                        part_i = 0;
                        a_index_off++;
                        t = index_t_compact[a_index_off];
                        a = index_a_compact[a_index_off];
                        t_read_index = get_long(index_t_data_pos_addr, a_index_off * 8);
                        t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                        t_data_temp = t_data_buf.slice(t_read_index, t_write_index);
                    }
                }
            }
            t_index_min++;
            if (t_index_min < t_index_max) {
                int t_index_max_1 = t_index_max - 1;
                for (; t_index_min < t_index_max_1; t_index_min++) {
                    a_index_min = t_index_min * T_A_RATE;
                    a_index_off = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
                    a_index_len = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
                    if (a_index_len > 0) {
                        int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                        long a_read_pos = index_a_data_pos[a_index_off];
                        int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + 1, a_index_cnt)] - a_read_pos);
                        a_read_buf.clear().limit(a_read_len);
                        do_read(a_read_channel, a_read_buf, a_read_pos);
                        long a = index_a_compact[a_index_off];
                        a_data_buf.clear_read();
                        for (int i = 0, part_i = 0; i < msg_size; ) {
                            a += a_data_buf.readVLong();
                            if (a >= min_a && a <= max_a) {
                                sum += a;
                                count++;
                            }
                            part_i++;
                            if (part_i == A_BLOCK) {
                                part_i = 0;
                                a_index_off++;
                                for (; i + A_BLOCK < msg_size; ) {
                                    int  a_min_max_pos = (a_index_off + 1) << 1;
                                    long a_min         = index_a_min_max[a_min_max_pos];
                                    long a_max         = index_a_min_max[a_min_max_pos + 1];
                                    if (a_min >= min_a && a_max <= max_a) {
                                        sum += index_a_sum[a_index_off + 1];
                                        count += A_BLOCK;
                                        i += A_BLOCK;
                                        a_index_off++;
                                        continue;
                                    } else {
                                        break;
                                    }
                                }
                                a_read_pos = index_a_data_pos[a_index_off];
                                a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + 1, a_index_cnt)] - a_read_pos);
                                a_read_buf.clear().limit(a_read_len);
                                do_read(a_read_channel, a_read_buf, a_read_pos);
                                a = index_a_compact[a_index_off];
                                a_data_buf.set_read_index(index_a_data_pos[a_index_off] - a_read_pos);
                            }
                            i++;
                        }
                    }
                }
                a_index_min = t_index_min * T_A_RATE;
                a_index_off = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
                a_index_len = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
                if (a_index_len > 0) {
                    int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                    long a_read_pos = index_a_data_pos[a_index_off];
                    int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + a_index_len + 1, a_index_cnt)] - a_read_pos);
                    a_read_buf.clear().limit(a_read_len);
                    do_read(a_read_channel, a_read_buf, a_read_pos);
                    long    t             = index_t_compact[a_index_off];
                    long    a             = index_a_compact[a_index_off];
                    long    t_read_index  = get_long(index_t_data_pos_addr, a_index_off * 8);
                    long    t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                    VIntBuf t_data_temp   = t_data_buf.slice(t_read_index, t_write_index);
                    a_data_buf.clear_read();
                    for (int i = 0, part_i = 0; i < msg_size; i++) {
                        t += t_data_temp.readZigZag();
                        a += a_data_buf.readVLong();
                        if (t >= min_t && t <= max_t && a >= min_a && a <= max_a) {
                            sum += a;
                            count++;
                        }
                        part_i++;
                        if (part_i == A_BLOCK) {
                            part_i = 0;
                            a_index_off++;
                            t = index_t_compact[a_index_off];
                            a = index_a_compact[a_index_off];
                            t_read_index = get_long(index_t_data_pos_addr, a_index_off * 8);
                            t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                            t_data_temp = t_data_buf.slice(t_read_index, t_write_index);
                        }
                    }
                }
            }
            if (count == 0) {
                return 0;
            }
            return sum / count;
        }

        List<Message> get_message(long min_a, long max_a, long min_t, long max_t) throws Exception {
            MFile         m_file                 = MFile.get();
            ByteBuffer    index_t_sparse         = m_file.index_t_sparse;
            ByteBuffer    index_t_data_pos       = m_file.index_t_data_pos;
            long[]        index_t_compact        = m_file.index_t_compact;
            long[]        index_a_min_max        = m_file.index_a_min_max;
            long[]        index_a_compact        = m_file.index_a_compact;
            long[]        index_a_data_pos       = m_file.index_a_data_pos;
            FileChannel   a_read_channel         = m_file.a_read_channel;
            FileChannel   b_read_channel         = m_file.b_read_channel;
            VIntBuf       t_data_buf             = this.t_data_buf;
            VLongBuf      a_data_buf             = this.a_data_buf;
            ByteBuffer    a_read_buf             = this.a_read_buf;
            ByteBuffer    b_read_buf             = this.b_read_buf;
            Message[]     msg_sort_buf           = MQueue.QUEUES[index].messages;
            long          b_read_buf_addr        = get_address(b_read_buf);
            long          index_t_data_pos_addr  = get_address(index_t_data_pos);
            long          index_t_sparse_addr    = get_address(index_t_sparse);
            int           index_t_data_pos_limit = index_t_data_pos.position();
            int           msg_sort_buf_size      = 0;
            int           t_size                 = m_file.t_size;
            int           t_index_cnt            = (index_t_sparse.position() >>> 3) - 1; // size -1
            int           t_index_min            = half_find(index_t_sparse_addr, 0, t_index_cnt, min_t);
            int           t_index_max            = get_t_index_max(index_t_sparse_addr, t_index_min, t_index_cnt, max_t);
            int           a_index_cnt            = (m_file.index_a_min_max_size >>> 1) - 1; // size -1
            int           a_index_min            = t_index_min * T_A_RATE;
            int           a_index_off            = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
            int           a_index_len            = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
            List<Message> msg_list               = new ArrayList<>(1024 * 8);
            if (a_index_len > 0) {
                int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                long a_read_pos = index_a_data_pos[a_index_off];
                long b_read_pos = 1L * (a_index_off * A_BLOCK) * B_LEN;
                int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + a_index_len + 1, a_index_cnt)] - a_read_pos);
                a_read_buf.clear().limit(a_read_len);
                b_read_buf.clear().limit(msg_size * B_LEN);
                do_read(a_read_channel, a_read_buf, a_read_pos);
                do_read(b_read_channel, b_read_buf, b_read_pos);
                long    t             = index_t_compact[a_index_off];
                long    a             = index_a_compact[a_index_off];
                long    t_read_index  = get_long(index_t_data_pos_addr, a_index_off * 8);
                long    t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                VIntBuf t_data_temp   = t_data_buf.slice(t_read_index, t_write_index);
                a_data_buf.clear_read();
                for (int i = 0, part_i = 0; i < msg_size; i++) {
                    t += t_data_temp.readZigZag();
                    a += a_data_buf.readVLong();
                    if (t >= min_t && t <= max_t && a >= min_a && a <= max_a) {
                        byte[] body = new byte[B_LEN];
                        copy_data(b_read_buf_addr, i * B_LEN, body, B_LEN);
                        msg_sort_buf[msg_sort_buf_size++] = new Message(a, t, body);
                    }
                    part_i++;
                    if (part_i == A_BLOCK) {
                        part_i = 0;
                        a_index_off++;
                        t = index_t_compact[a_index_off];
                        a = index_a_compact[a_index_off];
                        t_read_index = get_long(index_t_data_pos_addr, a_index_off * 8);
                        t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                        t_data_temp = t_data_buf.slice(t_read_index, t_write_index);
                    }
                }
                QuickSort.sort(msg_sort_buf, QuickSort.SORT_VALUE_T, 0, msg_sort_buf_size - 1);
                for (int i = 0; i < msg_sort_buf_size; i++) {
                    msg_list.add(msg_sort_buf[i]);
                }
                msg_sort_buf_size = 0;
            }
            t_index_min++;
            if (t_index_min < t_index_max) {
                int t_index_max_1 = t_index_max - 1;
                for (; t_index_min < t_index_max_1; t_index_min++) {
                    a_index_min = t_index_min * T_A_RATE;
                    a_index_off = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
                    a_index_len = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
                    if (a_index_len > 0) {
                        int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                        long a_read_pos = index_a_data_pos[a_index_off];
                        long b_read_pos = 1L * (a_index_off * A_BLOCK) * B_LEN;
                        int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + a_index_len + 1, a_index_cnt)] - a_read_pos);
                        a_read_buf.clear().limit(a_read_len);
                        b_read_buf.clear().limit(msg_size * B_LEN);
                        do_read(a_read_channel, a_read_buf, a_read_pos);
                        do_read(b_read_channel, b_read_buf, b_read_pos);
                        long    t             = index_t_compact[a_index_off];
                        long    a             = index_a_compact[a_index_off];
                        long    t_read_index  = get_long(index_t_data_pos_addr, a_index_off * 8);
                        long    t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                        VIntBuf t_data_temp   = t_data_buf.slice(t_read_index, t_write_index);
                        a_data_buf.clear_read();
                        for (int i = 0, part_i = 0; i < msg_size; i++) {
                            t += t_data_temp.readZigZag();
                            a += a_data_buf.readVLong();
                            if (a >= min_a && a <= max_a) {
                                byte[] body = new byte[B_LEN];
                                copy_data(b_read_buf_addr, i * B_LEN, body, B_LEN);
                                msg_sort_buf[msg_sort_buf_size++] = new Message(a, t, body);
                            }
                            part_i++;
                            if (part_i == A_BLOCK) {
                                part_i = 0;
                                a_index_off++;
                                t = index_t_compact[a_index_off];
                                a = index_a_compact[a_index_off];
                                t_read_index = get_long(index_t_data_pos_addr, a_index_off * 8);
                                t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                                t_data_temp = t_data_buf.slice(t_read_index, t_write_index);
                            }
                        }
                        QuickSort.sort(msg_sort_buf, QuickSort.SORT_VALUE_T, 0, msg_sort_buf_size - 1);
                        for (int i = 0; i < msg_sort_buf_size; i++) {
                            msg_list.add(msg_sort_buf[i]);
                        }
                        msg_sort_buf_size = 0;
                    }
                }
                a_index_min = t_index_min * T_A_RATE;
                a_index_off = get_a_read_index_off(index_a_min_max, a_index_cnt, a_index_min, min_a);
                a_index_len = get_a_read_index_len(index_a_min_max, a_index_cnt, a_index_min, a_index_off, max_a);
                if (a_index_len > 0) {
                    int  msg_size   = Math.min(t_size - (a_index_off) * A_BLOCK, a_index_len * A_BLOCK);
                    long a_read_pos = index_a_data_pos[a_index_off];
                    long b_read_pos = 1L * (a_index_off * A_BLOCK) * B_LEN;
                    int  a_read_len = (int) (index_a_data_pos[Math.min(a_index_off + a_index_len + 1, a_index_cnt)] - a_read_pos);
                    a_read_buf.clear().limit(a_read_len);
                    b_read_buf.clear().limit(msg_size * B_LEN);
                    do_read(a_read_channel, a_read_buf, a_read_pos);
                    do_read(b_read_channel, b_read_buf, b_read_pos);
                    long    t             = index_t_compact[a_index_off];
                    long    a             = index_a_compact[a_index_off];
                    long    t_read_index  = get_long(index_t_data_pos_addr, a_index_off * 8);
                    long    t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                    VIntBuf t_data_temp   = t_data_buf.slice(t_read_index, t_write_index);
                    a_data_buf.clear_read();
                    for (int i = 0, part_i = 0; i < msg_size; i++) {
                        t += t_data_temp.readZigZag();
                        a += a_data_buf.readVLong();
                        if (t >= min_t && t <= max_t && a >= min_a && a <= max_a) {
                            byte[] body = new byte[B_LEN];
                            copy_data(b_read_buf_addr, i * B_LEN, body, B_LEN);
                            msg_sort_buf[msg_sort_buf_size++] = new Message(a, t, body);
                        }
                        part_i++;
                        if (part_i == A_BLOCK) {
                            part_i = 0;
                            a_index_off++;
                            t = index_t_compact[a_index_off];
                            a = index_a_compact[a_index_off];
                            t_read_index = get_long(index_t_data_pos_addr, a_index_off * 8);
                            t_write_index = get_limit_index(index_t_data_pos_addr, index_t_data_pos_limit, (a_index_off + 1) * 8, t_data_buf.write_index);
                            t_data_temp = t_data_buf.slice(t_read_index, t_write_index);
                        }
                    }
                    QuickSort.sort(msg_sort_buf, QuickSort.SORT_VALUE_T, 0, msg_sort_buf_size - 1);
                    for (int i = 0; i < msg_sort_buf_size; i++) {
                        msg_list.add(msg_sort_buf[i]);
                    }
                    msg_sort_buf_size = 0;
                }
            }
            return msg_list;
        }

    }

    static int get_a_read_index_off(long[] min_max, int index_cnt, int a_index, long min_a) {
        int a_min_max_index = (a_index + 1);
        int a_block_len     = Math.min(index_cnt + 1 - a_min_max_index, T_A_RATE);
        for (int i = 0; i < a_block_len; i++) {
            int  a_min_pos = (a_min_max_index + i) * 2;
            long a_max     = min_max[a_min_pos + 1];
            if (a_max >= min_a) {
                return a_index + i;
            }
        }
        return a_index + a_block_len;
    }

    static int get_a_read_index_len(long[] min_max, int index_cnt, int a_index, int a_index_off, long max_a) {
        int a_min_max_index = (a_index + 1);
        int start           = a_index_off - a_index;
        int a_block_len     = Math.min(index_cnt + 1 - a_index, T_A_RATE);
        for (int i = start; i < a_block_len; i++) {
            int  a_min_pos = (a_min_max_index + i) * 2;
            long a_max     = min_max[a_min_pos + 1];
            if (a_max > max_a) {
                return i + 1 - start;
            }
        }
        return a_block_len - start;
    }

    static long get_limit_index(long addr, int buf_limit, int pos, long over_index) {
        if (pos < buf_limit) {
            return UNSAFE.getLong(addr + pos);
        } else {
            return over_index;
        }
    }

    static long do_read(FileChannel ch, ByteBuffer dst, long pos) throws IOException {
        int read = ch.read(dst, pos);
        if (dst.hasRemaining()) {
            log("read not complete");
            for (; dst.hasRemaining(); ) {
                Thread.yield();
                read = ch.read(dst, pos);
                if (read == -1) {
                    break;
                } else {
                    pos += read;
                }
            }
        }
        return pos;
    }

    static int get_t_index_max(long array, int low, int hi, long v) {
        for (int i = low; i <= hi; i++) {
            long find_v = get_long(array, i << 3);
            if (find_v > v) {
                return i;
            }
        }
        return hi + 1;
    }

    static int half_find(long array, int low, int hi, long v) {
        int mid = (low + hi) >>> 1;
        for (; low < hi; ) {
            long find_v = get_long(array, mid << 3);
            if (find_v == v) {
                break;
            }
            if (find_v > v) {
                hi = mid;
            } else {
                low = mid;
            }
            if (hi - low < 2) {
                return low;
            }
            mid = (low + hi) >>> 1;
        }
        if (get_long(array, 0) == v) {
            return 0;
        }
        for (; ; ) {
            if (get_long(array, (--mid) << 3) < v) {
                return mid;
            }
        }
    }

    static class MQueue {

        static final MQueue[]      QUEUES    = new MQueue[SEND_THREAD_NUM];
        static final AtomicInteger QUEUE_SEQ = new AtomicInteger();

        final int index;

        MQueue(int index) {
            this.index = index;
        }

        static void init() {
            for (int i = 0; i < SEND_THREAD_NUM; i++) {
                QUEUES[i] = new MQueue(i);
            }
        }

        static void destroy_all() {
            for (int i = 0; i < SEND_THREAD_NUM; i++) {
                QUEUES[i].destroy();
                QUEUES[i].next = null;
                QUEUES[i] = null;
            }
        }

        static int size() {
            return QUEUE_SEQ.get();
        }

        static MQueue next_queue() {
            int index = QUEUE_SEQ.getAndIncrement();
            return QUEUES[index];
        }

        final Message[] messages = new Message[MSG_SEND_CACHE_SIZE];

        int     size;
        int     read_index;
        int     write_index;
        boolean added;

        MQueue next;

        boolean has_next_message() {
            return size > 1;
        }

        boolean has_next_message_force() {
            return size > 0;
        }

        void add_message(Message message) {
            if (size == MSG_SEND_CACHE_SIZE) {
                log("message cache is full");
                throw new RuntimeException("message cache is full");
            }
            size++;
            messages[write_index++] = message;
            if (write_index == MSG_SEND_CACHE_SIZE) {
                write_index = 0;
            }
        }

        Message remove() {
            return remove0(1);
        }

        Message remove_force() {
            return remove0(0);
        }

        Message remove0(int remain) {
            if (size > remain) {
                size--;
                int     read_index = this.read_index;
                Message m          = messages[read_index];
                messages[read_index] = null;
                read_index++;
                if (read_index == MSG_SEND_CACHE_SIZE) {
                    read_index = 0;
                }
                this.read_index = read_index;
                return m;
            }
            return null;
        }

        Message curr_msg() {
            return messages[read_index];
        }

        void destroy() {
            Message[] messages = this.messages;
            for (int i = 0; i < MSG_SEND_CACHE_SIZE; i++) {
                Message temp = messages[i];
                temp.setBody(null);
                messages[i] = null;
            }
        }

        long cur_t() {
            return curr_msg().getT();
        }

        boolean has_next() {
            return next != null;
        }

    }

    static class IOThread extends Thread {

        static final BlockingQueue<ByteBuffer> BUFFERS            = new ArrayBlockingQueue<>(BUFFER_SIZE_SIZE * 4);
        static final IOThread                  IO_THREAD_INSTANCE = new IOThread(0);

        final BlockingQueue<WriteTask> tasks = new ArrayBlockingQueue<>(1024);
        final int                      io_thread_index;

        IOThread(int io_thread_index) {
            super("IOThread-" + io_thread_index);
            this.io_thread_index = io_thread_index;
        }

        static void init() {
            get().start();
        }

        static IOThread get() {
            return IO_THREAD_INSTANCE;
        }

        static ByteBuffer submit(FileChannel ch, ByteBuffer data, int write_size, CountDownLatch latch) {
            ByteBuffer buffer = take_buffer();
            buffer.clear();
            int pos = data.position();
            data.flip().position(write_size);
            buffer.put(data);
            data.clear().limit(write_size);
            get().offer(new WriteTask(ch, data, latch));
            return buffer;
        }

        void offer(WriteTask task) {
            tasks.offer(task);
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    WriteTask task = tasks.poll(1000, TimeUnit.SECONDS);
                    if (task == null) {
                        continue;
                    }
                    ByteBuffer  data    = task.buffer;
                    FileChannel channel = task.channel;
                    try {
                        for (; data.hasRemaining(); ) {
                            channel.write(data);
                        }
                    } catch (Exception e) {
                        printException(e);
                        throw new Error(e);
                    }
                    if (task.latch != null) {
                        task.latch.countDown();
                    }
                    IOThread.BUFFERS.offer(task.buffer);
                } catch (Exception e) {
                    printException(e);
                }
            }
        }

    }

    static class WriteTask {

        final CountDownLatch latch;
        final FileChannel    channel;
        final ByteBuffer     buffer;

        WriteTask(FileChannel channel, ByteBuffer buffer, CountDownLatch latch) {
            this.channel = channel;
            this.buffer = buffer;
            this.latch = latch;
        }
    }

}
