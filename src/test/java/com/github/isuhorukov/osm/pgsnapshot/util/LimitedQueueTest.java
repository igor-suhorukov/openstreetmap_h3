package com.github.isuhorukov.osm.pgsnapshot.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LimitedQueueTest {

    @Test
    void offer_belowCapacity_returnsTrue() {
        LimitedQueue<String> queue = new LimitedQueue<>(5);
        assertTrue(queue.offer("a"));
        assertTrue(queue.offer("b"));
        assertEquals(2, queue.size());
    }

    @Test
    void offer_atCapacity_blocks_thenReturnsTrueWhenSpaceAvailable() throws InterruptedException {
        LimitedQueue<String> queue = new LimitedQueue<>(1);
        queue.offer("first");

        Thread consumer = new Thread(() -> {
            try {
                Thread.sleep(50);
                queue.poll();
            } catch (InterruptedException ignored) {}
        });
        consumer.start();

        boolean result = queue.offer("second");
        consumer.join(2000);
        assertTrue(result);
    }

    @Test
    void offer_interrupted_returnsFalse() throws InterruptedException {
        LimitedQueue<String> queue = new LimitedQueue<>(1);
        queue.offer("full");

        Thread[] holder = new Thread[1];
        boolean[] result = {true};

        Thread blocker = new Thread(() -> {
            holder[0] = Thread.currentThread();
            result[0] = queue.offer("blocked");
        });
        blocker.start();

        Thread.sleep(50);
        blocker.interrupt();
        blocker.join(2000);

        assertFalse(result[0]);
    }
}
