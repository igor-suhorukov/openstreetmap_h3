package com.github.isuhorukov.osm.pgsnapshot.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

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
        boolean[] result = {false};

        Thread producer = new Thread(() -> result[0] = queue.offer("second"));
        producer.start();
        awaitWaiting(producer);
        assertEquals("first", queue.poll());
        producer.join(2000);

        assertTrue(result[0]);
        assertEquals("second", queue.peek());
    }

    @Test
    void offer_interrupted_returnsFalse() throws InterruptedException {
        LimitedQueue<String> queue = new LimitedQueue<>(1);
        queue.offer("full");
        boolean[] result = {true};

        Thread blocker = new Thread(() -> result[0] = queue.offer("blocked"));
        blocker.start();
        awaitWaiting(blocker);
        blocker.interrupt();
        blocker.join(2000);

        assertFalse(result[0]);
    }

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.WAITING, thread.getState());
    }
}
