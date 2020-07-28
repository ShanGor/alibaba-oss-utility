package com.aliyun.demo.util;

import org.junit.Test;

public class TimeKeeperTest {
    @Test
    public void testIt() throws InterruptedException {
        TimeKeeper tk = new TimeKeeper();
        Thread.sleep(20);
        System.out.println(tk.elapsedMilliSeconds());
        tk.reset();
        Thread.sleep(20);
        System.out.println(tk.elapsedSeconds());
    }
}
