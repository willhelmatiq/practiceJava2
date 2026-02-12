package com.example.task3;

import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

public class RingBuffer<E> {
    private static final int DEFAULT_SIZE = 10;
    private int size;
    private int head;
    private int tail;
    private final Object[] innerArray;

    RingBuffer() {
        this(DEFAULT_SIZE);
    }

    RingBuffer(int size) {
        this.head = 0;
        this.tail = 0;
        this.size = 0;
        this.innerArray = new Object[size];
    }

    public synchronized void add(E e) {
        if (size != 0) {
            moveTail();
            if (head == tail) {
                moveHead();
                size--;
            }
        }
        innerArray[tail] = e;
        size++;
    }

    public synchronized E poll() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        E element = (E) innerArray[head];
        innerArray[head] = null;
        if (head != tail) {
            moveHead();
        }
        size--;
        return element;
    }

    public synchronized E peek() {
        if (size == 0) {
            throw new NoSuchElementException();
        }
        return (E) innerArray[head];
    }

    public synchronized int size() {
        return size;
    }

    public int capacity() {
        return innerArray.length;
    }

    private void moveHead() {
        if (head < innerArray.length - 1) {
            head++;
        } else {
            head = 0;
        }
    }

    private void moveTail() {
        if (tail < innerArray.length - 1) {
            tail++;
        } else {
            tail = 0;
        }
    }
}
