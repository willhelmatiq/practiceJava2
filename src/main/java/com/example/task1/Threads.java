package com.example.task1;

public class Threads {
    public static void main(String[] args) {
        Object lock = new Object();
        int[] value = new int[1];
        PrintingNubmerThread evenTread = new PrintingNubmerThread(value, true, lock);
        PrintingNubmerThread oddTread = new PrintingNubmerThread(value, false, lock);
        evenTread.start();
        oddTread.start();
    }

    static class PrintingNubmerThread extends Thread {
        private int[] value;
        private boolean isEven;
        private Object lock;

        PrintingNubmerThread(int[] value, boolean isEven, Object lock) {
            this.value = value;
            this.isEven = isEven;
            this.lock = lock;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (lock) {
                    if (value[0] % 2 == 0 && this.isEven) {
                        System.out.println(value[0] + " :" + Thread.currentThread().getName());
                        value[0]++;
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                synchronized (lock) {
                    if (value[0] % 2 == 1 && !this.isEven) {
                        System.out.println(value[0] + " :" + Thread.currentThread().getName());
                        value[0]++;
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }
}
