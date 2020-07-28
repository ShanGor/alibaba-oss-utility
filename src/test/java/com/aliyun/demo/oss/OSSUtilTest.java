package com.aliyun.demo.oss;

import org.junit.Assert;
import org.junit.Test;
import static java.lang.System.out;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class OSSUtilTest {
//    @Test
    public void testMd5sumBase64() throws IOException {
        String result = OSSUtil.md5sumBase64("/Users/sam/Downloads/ideaIC-2020.1.1.dmg");
        Assert.assertEquals("eSsiTLT6eCVheoV9pfNB+g==", result);
        out.println(result);
    }

//    @Test
    public void temp1() throws IOException {
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream(pos, 10);
        pos.write("are you olkay, my god".getBytes());
        pos.write("second".getBytes());
    }

//    @Test
    public void testUpload() throws IOException {
        try(OSSUtil util = new OSSUtil();) {
            util.getOssClient().deleteObject("insurance-poc-idp", "interface/test_partition.csv.gz");
            util.upload("insurance-poc-idp", "interface/test_partition.csv.gz", "/tmp/test_partition.csv.gz");
        }
    }

//    @Test
    public void testUploadLarge() throws IOException {
        try(OSSUtil util = new OSSUtil();) {
            util.getOssClient().deleteObject("insurance-poc-idp", "interface/test_partition.csv");
            util.uploadLarge("insurance-poc-idp", "interface/test_partition.csv", "/tmp/test_partition.csv");
        }
    }

//    @Test
    public void generateTestData() throws IOException {
        BufferedWriter bw = Files.newBufferedWriter(Paths.get("/tmp/test_partition.csv"));
        bw.write("a,b,c\n");
        for (int i=1; i<=100_000_000; i++) {
            bw.write("are, you, " + i + "\n");
        }
    }
}
