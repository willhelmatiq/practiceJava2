package com.example.task4.mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Worker implements Runnable {

    private final Coordinator coordinator;
    private static final int DEFAULT_SLEEP_TIME_MS = 10;
    private final int sleepTimeMs;

    public Worker(Coordinator coordinator) {
        this.coordinator = coordinator;
        this.sleepTimeMs = DEFAULT_SLEEP_TIME_MS;
    }

    public Worker(Coordinator coordinator, int sleepTimeMs) {
        this.coordinator = coordinator;
        this.sleepTimeMs = sleepTimeMs;
    }


    @Override
    public void run() {

        while (true) {

            Task task = coordinator.requestTask();

            switch (task.getType()) {
                case MAP -> executeMap(task);
                case REDUCE -> executeReduce(task);
                case WAIT -> sleep();
                case EXIT -> {
                    System.out.println("Thread: " + Thread.currentThread().getName() + " exit");
                    return;
                }
            }
        }
    }

    private void sleep() {
        System.out.println("Thread: " + Thread.currentThread().getName() + " sleep");
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void executeMap(Task task) {
        System.out.println("Thread: " + Thread.currentThread().getName() + " executeMap");
        try {
            String content = Files.readString(Path.of(task.getFileName()));
            List<KeyValue> keyValues = map(content);

            writeIntermediateFiles(
                    task.getTaskId(),
                    task.getReduceCount(),
                    keyValues
            );

            coordinator.reportDone(task);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<KeyValue> map(String content) {
        List<KeyValue> result = new ArrayList<>();
        String[] words = content.split("\\W+");

        for (String word : words) {
            if (!word.isBlank()) {
                result.add(new KeyValue(word.toLowerCase(), "1"));
            }
        }

        return result;
    }

    private void writeIntermediateFiles(
            int mapId,
            int reduceCount,
            List<KeyValue> keyValues
    ) throws IOException {

        Map<Integer, List<KeyValue>> buckets = new HashMap<>();

        for (KeyValue kv : keyValues) {

            int bucket = Math.floorMod(kv.key().hashCode(), reduceCount);

            buckets
                    .computeIfAbsent(bucket, k -> new ArrayList<>())
                    .add(kv);
        }

        for (Map.Entry<Integer, List<KeyValue>> entry : buckets.entrySet()) {

            int reduceId = entry.getKey();
            List<KeyValue> values = entry.getValue();

            String fileName = "mr-" + mapId + "-" + reduceId;

            try (BufferedWriter writer = Files.newBufferedWriter(Path.of(fileName))) {
                for (KeyValue kv : values) {
                    writer.write(kv.key() + " " + kv.value());
                    writer.newLine();
                }
            }
        }
    }

    private void executeReduce(Task task) {
        System.out.println("Thread: " + Thread.currentThread().getName() + " executeReduce");
        try {
            Map<String, List<String>> grouped = new HashMap<>();

            for (String fileName : task.getIntermediateFiles()) {
                Path path = Path.of(fileName);

                if (!Files.exists(path)) {
                    continue;
                }

                List<String> lines = Files.readAllLines(path);

                for (String line : lines) {

                    String[] parts = line.split(" ");
                    String key = parts[0];
                    String value = parts[1];

                    grouped
                            .computeIfAbsent(key, k -> new ArrayList<>())
                            .add(value);
                }
            }

            writeReduceOutput(task.getReduceId(), grouped);

            coordinator.reportDone(task);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeReduceOutput(
            int reduceId,
            Map<String, List<String>> grouped
    ) throws IOException {

        List<String> sortedKeys = new ArrayList<>(grouped.keySet());
        Collections.sort(sortedKeys);

        String outputFile = "mr-out-" + reduceId;

        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(outputFile))) {

            for (String key : sortedKeys) {

                String result = reduce(key, grouped.get(key));

                writer.write(key + " " + result);
                writer.newLine();
            }
        }
    }

    private String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }
}
