package com.example.task4;


import com.example.task4.mapreduce.Coordinator;
import com.example.task4.mapreduce.Worker;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> inputFiles = List.of(
                "src/main/resources/file1.txt",
                "src/main/resources/file2.txt"
        );

        int reduceCount = 5;

        Coordinator coordinator = new Coordinator(inputFiles, reduceCount);

        int workerCount = 3;

        for (int i = 0; i < workerCount; i++) {
            new Thread(new Worker(coordinator)).start();
        }
    }
}
