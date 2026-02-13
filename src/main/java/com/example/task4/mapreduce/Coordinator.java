package com.example.task4.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Coordinator {

    private final List<String> inputFiles;
    private final int reduceCount;

    private final Map<Integer, TaskStatus> mapTasks = new HashMap<>();
    private final Map<Integer, TaskStatus> reduceTasks = new HashMap<>();

    private Phase phase = Phase.MAP;

    public Coordinator(List<String> inputFiles, int reduceCount) {
        this.inputFiles = inputFiles;
        this.reduceCount = reduceCount;

        initMapTasks();
    }

    public synchronized Task requestTask() {

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

    public synchronized void reportDone(Task task) {
        if (task.getType() == TaskType.MAP) {
            mapTasks.put(task.getTaskId(), TaskStatus.COMPLETED);
        } else if (task.getType() == TaskType.REDUCE) {
            reduceTasks.put(task.getReduceId(), TaskStatus.COMPLETED);
        }
    }

    private Task handleMapPhase() {

        // ищем READY map задачу
        for (Map.Entry<Integer, TaskStatus> entry : mapTasks.entrySet()) {
            if (entry.getValue() == TaskStatus.READY) {

                int taskId = entry.getKey();
                mapTasks.put(taskId, TaskStatus.IN_PROGRESS);

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
}