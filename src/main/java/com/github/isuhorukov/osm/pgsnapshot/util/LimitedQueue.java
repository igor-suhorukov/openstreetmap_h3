package com.github.isuhorukov.osm.pgsnapshot.util;

import java.util.concurrent.LinkedBlockingQueue;

public class LimitedQueue <E> extends LinkedBlockingQueue<E>
{
    public LimitedQueue(int maxSize)
    {
        super(maxSize);
    }

    @Override
    public boolean offer(E e)
    {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            put(e);
            return true;
        } catch(InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

}