package com.example.task1;

public class Threads {
    public static void main(String[] args) throws InterruptedException {
        Object lock = new Object();
        int[] value = new int[1];
        int limit = 100;
        Thread evenThread = new PrintingNumberThread(value, true, lock, limit);
        Thread oddThread = new PrintingNumberThread(value, false, lock, limit);

        evenThread.start();
        oddThread.start();

        evenThread.join();
        oddThread.join();

        System.out.println("Finished");
    }

    static class PrintingNumberThread extends Thread {

        private final int[] value;
        private final boolean isEven;
        private final Object lock;
        private final int limit;

        PrintingNumberThread(int[] value, boolean isEven, Object lock, int limit) {
            this.value = value;
            this.isEven = isEven;
            this.lock = lock;
            this.limit = limit;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (lock) {

                    if (value[0] > limit) {
                        lock.notifyAll(); // будим другой поток
                        return;
                    }

                    while ((value[0] % 2 == 0) != isEven) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    if (value[0] <= limit) {
                        System.out.println(value[0] + " : " + Thread.currentThread().getName());
                        value[0]++;
                        lock.notifyAll();
                    }
                }
            }
        }
    }
}
