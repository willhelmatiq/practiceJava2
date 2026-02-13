package com.example.task4.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Coordinator {

    private final List<String> inputFiles;
    private final int reduceCount;

    private final Map<Integer, Integer> mapRetryCount = new HashMap<>();
    private final Map<Integer, Integer> reduceRetryCount = new HashMap<>();
    private static final int MAX_RETRIES = 3;

    private final Map<Integer, TaskStatus> mapTasks = new HashMap<>();
    private final Map<Integer, TaskStatus> reduceTasks = new HashMap<>();

    private final Map<Integer, Long> mapStartTime = new HashMap<>();
    private final Map<Integer, Long> reduceStartTime = new HashMap<>();
    private static final long TASK_TIMEOUT_MS = 3000;

    private Phase phase = Phase.MAP;

    public Coordinator(List<String> inputFiles, int reduceCount) {
        this.inputFiles = inputFiles;
        this.reduceCount = reduceCount;

        initMapTasks();
    }

    public synchronized Task requestTask() {
        checkMapTimeouts();
        checkReduceTimeouts();

        switch (phase) {
            case MAP -> {
                return handleMapPhase();
            }
            case REDUCE -> {
                return handleReducePhase();
            }
            case DONE -> {
                return new Task(TaskType.EXIT);
            }
        }

        throw new IllegalStateException();
    }

    public synchronized void reportSuccess(Task task) {
        if (task.getType() == TaskType.MAP) {
            int taskId = task.getTaskId();
            mapTasks.put(taskId, TaskStatus.COMPLETED);
            mapRetryCount.remove(taskId);
            mapStartTime.remove(taskId);
        }
        else if (task.getType() == TaskType.REDUCE) {
            int reduceId = task.getReduceId();
            reduceTasks.put(reduceId, TaskStatus.COMPLETED);
            reduceRetryCount.remove(reduceId);
            reduceStartTime.remove(reduceId);
        }
    }

    public synchronized void reportFailure(Task task) {
        if (task.getType() == TaskType.MAP) {

            int taskId = task.getTaskId();
            int count = mapRetryCount.getOrDefault(taskId, 0);

            if (count < MAX_RETRIES) {
                mapRetryCount.put(taskId, count + 1);
                mapTasks.put(taskId, TaskStatus.READY);
            } else {
                System.out.println("MAP task " + taskId + " exceeded retries");
                phase = Phase.DONE;
            }

        } else if (task.getType() == TaskType.REDUCE) {

            int reduceId = task.getReduceId();
            int count = reduceRetryCount.getOrDefault(reduceId, 0);

            if (count < MAX_RETRIES) {
                reduceRetryCount.put(reduceId, count + 1);
                reduceTasks.put(reduceId, TaskStatus.READY);
            } else {
                System.out.println("REDUCE task " + reduceId + " exceeded retries");
                phase = Phase.DONE;
            }
        }
    }

    private Task handleMapPhase() {

        // ищем READY map задачу
        for (Map.Entry<Integer, TaskStatus> entry : mapTasks.entrySet()) {
            if (entry.getValue() == TaskStatus.READY) {

                int taskId = entry.getKey();
                mapTasks.put(taskId, TaskStatus.IN_PROGRESS);
                mapStartTime.put(taskId, System.currentTimeMillis());

                return Task.mapTask(
                        taskId,
                        inputFiles.get(taskId),
                        reduceCount
                );
            }
        }

        // если есть задачи в процессе — ждём
        if (mapTasks.containsValue(TaskStatus.IN_PROGRESS)) {
            return new Task(TaskType.WAIT);
        }

        // сюда попадём только если нет READY и нет IN_PROGRESS
        initReduceTasks();
        phase = Phase.REDUCE;

        return handleReducePhase();
    }

    private Task handleReducePhase() {

        for (Map.Entry<Integer, TaskStatus> entry : reduceTasks.entrySet()) {

            if (entry.getValue() == TaskStatus.READY) {

                int reduceId = entry.getKey();
                reduceTasks.put(reduceId, TaskStatus.IN_PROGRESS);
                reduceStartTime.put(reduceId, System.currentTimeMillis());

                List<String> files = buildIntermediateFileList(reduceId);

                return Task.reduceTask(reduceId, files);
            }
        }

        if (reduceTasks.containsValue(TaskStatus.IN_PROGRESS)) {
            return new Task(TaskType.WAIT);
        }

        // сюда попадём только если нет READY и нет IN_PROGRESS
        phase = Phase.DONE;
        return new Task(TaskType.EXIT);
    }

    private void initMapTasks() {
        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.put(i, TaskStatus.READY);
        }
    }

    private void initReduceTasks() {
        for (int i = 0; i < reduceCount; i++) {
            reduceTasks.put(i, TaskStatus.READY);
        }
    }

    private List<String> buildIntermediateFileList(int reduceId) {

        List<String> files = new ArrayList<>();

        for (int mapId = 0; mapId < inputFiles.size(); mapId++) {
            files.add("mr-" + mapId + "-" + reduceId);
        }

        return files;
    }

    private void checkMapTimeouts() {
        long now = System.currentTimeMillis();

        for (Map.Entry<Integer, TaskStatus> entry : mapTasks.entrySet()) {
            int taskId = entry.getKey();
            if (entry.getValue() == TaskStatus.IN_PROGRESS) {
                long startedAt = mapStartTime.getOrDefault(taskId, 0L);
                if (now - startedAt > TASK_TIMEOUT_MS) {
                    System.out.println("MAP task " + taskId + " timed out");

                    mapTasks.put(taskId, TaskStatus.READY);
                    mapStartTime.remove(taskId);
                }
            }
        }
    }

    private void checkReduceTimeouts() {
        long now = System.currentTimeMillis();

        for (Map.Entry<Integer, TaskStatus> entry : reduceTasks.entrySet()) {
            int reduceId = entry.getKey();
            if (entry.getValue() == TaskStatus.IN_PROGRESS) {
                long startedAt = reduceStartTime.getOrDefault(reduceId, 0L);
                if (now - startedAt > TASK_TIMEOUT_MS) {
                    System.out.println("REDUCE task " + reduceId + " timed out");

                    reduceTasks.put(reduceId, TaskStatus.READY);
                    reduceStartTime.remove(reduceId);
                }
            }
        }
    }
}