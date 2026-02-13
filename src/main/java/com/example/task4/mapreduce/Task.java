package com.example.task4.mapreduce;

import lombok.Getter;

import java.util.List;

@Getter
public class Task {
    private final TaskType type;

    // для MAP
    private final int taskId;
    private final String fileName;
    private final int reduceCount;

    // для REDUCE
    private final int reduceId;
    private final List<String> intermediateFiles;

    // приватный конструктор
    private Task(TaskType type,
                 int taskId,
                 String fileName,
                 int reduceId,
                 List<String> intermediateFiles,
                 int reduceCount) {

        this.type = type;
        this.taskId = taskId;
        this.fileName = fileName;
        this.reduceId = reduceId;
        this.intermediateFiles = intermediateFiles;
        this.reduceCount = reduceCount;
    }

    // Factory для MAP
    public static Task mapTask(int taskId, String fileName, int reduceCount) {
        return new Task(
                TaskType.MAP,
                taskId,
                fileName,
                -1,
                null,
                reduceCount
        );
    }

    // Factory для REDUCE
    public static Task reduceTask(int reduceId, List<String> files) {
        return new Task(
                TaskType.REDUCE,
                -1,
                null,
                reduceId,
                files,
                0
        );
    }

    // Factory для WAIT и EXIT
    public Task(TaskType type) {
        this(type, -1, null, -1, null, 0);
    }

}
