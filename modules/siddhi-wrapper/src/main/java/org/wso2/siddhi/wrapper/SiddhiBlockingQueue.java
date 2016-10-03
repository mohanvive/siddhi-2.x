package org.wso2.siddhi.wrapper;


import org.wso2.siddhi.core.util.collection.queue.ISiddhiQueue;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SiddhiBlockingQueue<T> implements ISiddhiQueue<T> {
    private BlockingQueue<T> queue;
    private int size;
    private int count = 0;

    public SiddhiBlockingQueue(int size) {
        queue = new LinkedBlockingQueue<T>();

    }

    public synchronized void put(T t) {
        queue.add(t);
    }

    public synchronized T poll() {
        return queue.poll();
    }

    public synchronized T peek() {
        return queue.peek();
    }

    public Iterator<T> iterator() {
        return queue.iterator();
    }

    public Object[] currentState() {
        return new Object[]{queue};
    }

    public void restoreState(Object[] objects) {
        queue = (LinkedBlockingQueue) objects[0];
    }

    public int size() {
        return queue.size();
    }
}

