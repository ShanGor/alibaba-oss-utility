package com.aliyun.demo.util;

public class TimeKeeper {
    private long startTime;
    private long endTime;
    public TimeKeeper() {
        startTime = System.nanoTime();
    }

    public double elapsedSeconds() {
        endTime = System.nanoTime();
        return (endTime - startTime) / 1000_000_000.0;
    }

    public double elapsedMilliSeconds() {
        endTime = System.nanoTime();
        return (endTime - startTime) / 1000_000.0;
    }

    public void reset() {
        startTime = System.nanoTime();
    }
}
