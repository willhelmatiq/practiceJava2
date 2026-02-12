package com.example.task3;

public class Task3 {
    public static void main(String[] args) {
        RingBuffer<Integer> ringBuffer = new RingBuffer<>(3);
        ringBuffer.add(1);
        ringBuffer.add(2);
        ringBuffer.add(3);
        ringBuffer.add(4);
        ringBuffer.add(5);
        System.out.println(ringBuffer.peek());
        System.out.println(ringBuffer.poll());
        System.out.println(ringBuffer.peek());
        System.out.println(ringBuffer.poll());
        System.out.println(ringBuffer.peek());
        System.out.println(ringBuffer.poll());
    }
}
