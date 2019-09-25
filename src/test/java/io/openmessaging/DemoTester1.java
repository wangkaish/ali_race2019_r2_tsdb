package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


//这是评测程序的一个demo版本，其评测逻辑与实际评测程序基本类似，但是比实际评测简单很多
//该评测程序主要便于选手在本地优化和调试自己的程序

public class DemoTester1 {

    public static void main(String args[]) throws Exception {

        //        Util.sleep(10000);

        test();


        System.exit(0);
    }

    static void test() throws Exception {
        //评测相关配置
        //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
        int msgNum = DefaultMessageStoreImpl.MSG_SIZE / 100;
        //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
        int sendTime = 10 * 60 * 1000;
        //查询阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
        int checkTime = 10 * 60 * 1000;


        //正确性检测的次数
        int msgCheckTimes1 = 100;
        //正确性检测的次数
        int avgCheckTimes1 = 1000;
        //发送的线程数量
        int sendTsNum = DefaultMessageStoreImpl.SEND_THREAD_NUM;
        //查询的线程数量
        int checkTsNum = DefaultMessageStoreImpl.SEND_THREAD_NUM;
        // 每次查询消息的最大跨度
        int maxMsgCheckSize = 100000;
        // 每次查询求平均的最大跨度
        int maxValueCheckSize = 100000;

        MessageStore messageStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultMessageStoreImpl");
            messageStore = (MessageStore) queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }

        //Step1: 发送消息
        long       sendStart    = System.currentTimeMillis();
        long       maxTimeStamp = System.currentTimeMillis() + sendTime;
        AtomicLong sendCounter  = new AtomicLong(0);
        Thread[]   sends        = new Thread[sendTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            sends[i] = new Thread(new Producer(messageStore, maxTimeStamp, msgNum, sendCounter));
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].join();
        }
        long sendSend = System.currentTimeMillis();
        System.out.printf("Send: %d ms Num:%d\n", sendSend - sendStart, msgNum);
        long maxCheckTime = System.currentTimeMillis() + checkTime;


        //Step2: 查询聚合消息 ---------------------------------------------------------
        long       msgCheckStart = System.currentTimeMillis();
        AtomicLong msgCheckTimes = new AtomicLong(0);
        AtomicLong msgCheckNum   = new AtomicLong(0);
        Thread[]   msgChecks     = new Thread[checkTsNum];
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i] = new Thread(new MessageChecker(messageStore, maxCheckTime, msgCheckTimes1, msgNum, maxMsgCheckSize, msgCheckTimes, msgCheckNum));
        }
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i].start();
        }
        for (int i = 0; i < checkTsNum; i++) {
            msgChecks[i].join();
        }
        long msgCheckEnd = System.currentTimeMillis();
        System.out.printf("Message Check: %d ms Num:%d\n", msgCheckEnd - msgCheckStart, msgCheckNum.get());
        //Step2: 查询聚合消息 ---------------------------------------------------------

        //Step3: 查询聚合结果 ---------------------------------------------------------
        long       checkStart      = System.currentTimeMillis();
        AtomicLong valueCheckTimes = new AtomicLong(0);
        AtomicLong valueCheckNum   = new AtomicLong(0);
        Thread[]   checks          = new Thread[checkTsNum];
        for (int i = 0; i < checkTsNum; i++) {
            checks[i] = new Thread(new ValueChecker(messageStore, maxCheckTime, avgCheckTimes1, msgNum, maxValueCheckSize, valueCheckTimes, valueCheckNum));
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].start();
        }
        for (int i = 0; i < checkTsNum; i++) {
            checks[i].join();
        }
        long checkEnd = System.currentTimeMillis();
        System.out.printf("Value Check: %d ms Num: %d\n", checkEnd - checkStart, valueCheckNum.get());
        //Step3: 查询聚合结果 ---------------------------------------------------------


        //评测结果
        System.out.printf("Total Score:%d\n", (msgNum / (sendSend - sendStart) + msgCheckNum.get() / (msgCheckEnd - msgCheckStart) + valueCheckNum.get() / (msgCheckEnd - msgCheckStart)));
    }

    static class Producer implements Runnable {

        private AtomicLong   counter;
        private long         maxMsgNum;
        private MessageStore messageStore;
        private long         maxTimeStamp;

        public Producer(MessageStore messageStore, long maxTimeStamp, int maxMsgNum, AtomicLong counter) {
            this.counter = counter;
            this.maxMsgNum = maxMsgNum;
            this.messageStore = messageStore;
            this.maxTimeStamp = maxTimeStamp;
        }

        static final byte[] DATA = "01234567890123456789123456".getBytes();

        @Override
        public void run() {
            long count;
            while ((count = counter.getAndIncrement()) < maxMsgNum && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(34);
                    buffer.putLong(count);
                    buffer.put(DATA);
                    // 为测试方便, 插入的是有规律的数据, 不是实际测评的情况
                    messageStore.put(new Message(count, count, buffer.array()));
                    //                    if ((count & 0x1L) == 0) {
                    //                        //偶数count多加一条消息
                    //                        messageStore.put(new Message(count, count, buffer.array()));
                    //                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    static class MessageChecker implements Runnable {

        private AtomicLong   timesCounter;
        private AtomicLong   numCounter;
        private long         checkTimes;
        private MessageStore messageStore;
        private long         maxTimeStamp;
        private int          maxIndex;
        private int          maxCheckSize;

        public MessageChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize, AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp = maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError() {
            System.out.println("message check error");
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    long aIndex1 = random.nextInt(maxIndex);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    long aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);
                    long tIndex1 = random.nextInt((int) (aIndex2 - aIndex1)) + aIndex1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    long tIndex2 = random.nextInt(maxCheckSize) + tIndex1;
                    long index1  = Math.max(aIndex1, tIndex1);
                    long index2  = Math.min(aIndex2, tIndex2);
                    get_and_check(index1, index2, aIndex1, aIndex2, tIndex1, tIndex2);
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
            long aIndex1 = 0;
            long aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);
            long tIndex1 = 0;
            long tIndex2 = random.nextInt(maxCheckSize) + tIndex1;
            long index1  = Math.max(aIndex1, tIndex1);
            long index2  = Math.min(aIndex2, tIndex2);
            get_and_check(index1, index2, aIndex1, aIndex2, tIndex1, tIndex2);
        }

        void get_and_check(long index1, long index2, long aIndex1, long aIndex2, long tIndex1, long tIndex2) {
            List<Message> msgs = messageStore.getMessage(aIndex1, aIndex2, tIndex1, tIndex2);
            //验证消息
            Iterator<Message> iter = msgs.iterator();
            while (iter.hasNext()) {
                if (index1 > index2) {
                    checkError();
                }
                Message msg = iter.next();
                if (msg.getA() != msg.getT() || msg.getA() != index1 || ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
                    checkError();
                }
                //                //偶数需要多验证一次
                //                if ((index1 & 0x1) == 0 && iter.hasNext()) {
                //                    msg = iter.next();
                //                    if (msg.getA() != msg.getT() || msg.getA() != index1 || ByteBuffer.wrap(msg.getBody()).getLong() != index1) {
                //                        checkError();
                //                    }
                //                }
                ++index1;
            }
            if (index1 - 1 != index2) {
                checkError();
            }
            numCounter.getAndAdd(msgs.size());

        }

    }

    static class ValueChecker implements Runnable {

        private AtomicLong   timesCounter;
        private AtomicLong   numCounter;
        private long         checkTimes;
        private MessageStore messageStore;
        private long         maxTimeStamp;
        private int          maxIndex;
        private int          maxCheckSize;

        public ValueChecker(MessageStore messageStore, long maxTimeStamp, int checkTimes, int maxIndex, int maxCheckSize, AtomicLong timesCounter, AtomicLong numCounter) {
            this.timesCounter = timesCounter;
            this.numCounter = numCounter;
            this.checkTimes = checkTimes;
            this.messageStore = messageStore;
            this.maxTimeStamp = maxTimeStamp;
            this.maxIndex = maxIndex;
            this.maxCheckSize = maxCheckSize;
        }

        private void checkError(long aMin, long aMax, long tMin, long tMax, long res, long val) {
            System.out.printf("value check error. aMin:%d, aMax:%d, tMin:%d, tMax:%d, res:%d, val:%d\n", aMin, aMax, tMin, tMax, res, val);
            System.exit(-1);
        }

        @Override
        public void run() {
            Random random = new Random();
            while (timesCounter.getAndIncrement() < checkTimes && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    int aIndex1 = random.nextInt(maxIndex);
                    if (aIndex1 < 0) {
                        aIndex1 = 0;
                    }
                    int aIndex2 = Math.min(aIndex1 + maxCheckSize, maxIndex - 1);

                    int tIndex1 = random.nextInt(aIndex2 - aIndex1 + 1) + aIndex1 - 1;
                    if (tIndex1 < 0) {
                        tIndex1 = 0;
                    }
                    int tIndex2 = random.nextInt(maxCheckSize) + tIndex1;
                    int index1  = Math.max(aIndex1, tIndex1);
                    int index2  = Math.min(aIndex2, tIndex2);

                    get_and_check_avg(index1, index2, aIndex1, aIndex2, tIndex1, tIndex2);
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }

        void get_and_check_avg(long index1, long index2, long aIndex1, long aIndex2, long tIndex1, long tIndex2) {

            long val = messageStore.getAvgValue(aIndex1, aIndex2, tIndex1, tIndex2);
            //验证
            long evenIndex1 = index1;
            long evenIndex2 = index2;

            long avg   = 0;
            long count = 0;
            if (evenIndex1 <= evenIndex2) {
                long sum        = 0;
                long evenIndex3 = evenIndex2 + 1;
                for (long i = evenIndex1; i < evenIndex3; i++) {
                    sum += i;
                }
                count = (evenIndex3 - evenIndex1);
                avg = sum / count;
            }
            if (avg != val) {
                checkError(aIndex1, aIndex2, tIndex1, tIndex2, avg, val);
            }

            numCounter.getAndAdd(count);

        }

    }
}


