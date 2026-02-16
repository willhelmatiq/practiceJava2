package com.example.task3;

public class Task3 {
    public static void main(String[] args) {
        RingBuffer<Integer> ringBuffer = new RingBuffer<>(3);

        Thread producer = new Thread(() -> {
            int i = 0;
            while (i < 20) {
                try {
                    ringBuffer.add(i);
                    System.out.println("Produced: " + i);
                    i++;
                } catch (IllegalStateException e) {
                    System.out.println("Producer: buffer full");
                }
                sleep(200);
            }
        });

        Thread consumer = new Thread(() -> {
            int count = 0;
            while (count < 20) {
                try {
                    Integer value = ringBuffer.poll();
                    System.out.println("Consumed: " + value);
                    count++;
                } catch (Exception ignored) {
                    System.out.println("Consumer: " + "value not ready");
                }
                sleep(300);
            }
        });

        producer.start();
        consumer.start();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
