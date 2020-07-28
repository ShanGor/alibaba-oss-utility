package com.aliyun.demo.oss;


import java.util.List;
import java.util.ArrayList;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.*;
import org.apache.log4j.Logger;

public class MoveFile {

    private static Logger logger = Logger.getLogger(OSSUtil.class);


    public void wechatLogMove(OSS ossClient, String bucketName, String destinationObjectName, String marker) {
        logger.info("wechatLogMove begin:");
        try {
            ObjectListing objectListing = ossClient.listObjects(new ListObjectsRequest(bucketName).withMarker(marker));
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();

            for (OSSObjectSummary s : sums) {
                SimplifiedObjectMeta objectMeta = ossClient.getSimplifiedObjectMeta(bucketName, s.getKey());
                if (s.getKey().contains(marker)) {
                    String fileName = s.getKey().substring(s.getKey().lastIndexOf("/") + 1);
                    String destinationFileName = destinationObjectName + fileName;

                    if (objectMeta.getSize() > 1024 * 1024 * 1024) {
                        System.out.println("over 1G move:" + objectMeta.getSize());
                        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, destinationFileName);
                        InitiateMultipartUploadResult initiateMultipartUploadResult = ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
                        String uploadId = initiateMultipartUploadResult.getUploadId();

                        final long partSize = 1000 * 1024 * 1024L;   // 1000MB
                        ObjectMetadata metadata = ossClient.getObjectMetadata(bucketName, s.getKey());
                        long objectSize = metadata.getContentLength();
                        int partCount = (int) (objectSize / partSize);
                        if (objectSize % partSize != 0) {
                            partCount++;
                        }
                        if (partCount > 10000) {
                            throw new RuntimeException("Total parts count should not exceed 10000");
                        } else {
                            System.out.println("Total parts count " + partCount + "\n");
                        }
                        /*
                         * Upload multiparts by copy mode
                         */
                        System.out.println("Begin to upload multiparts by copy mode to OSS\n");
                        List<PartETag> partETags = new ArrayList<PartETag>();
                        for (int i = 0; i < partCount; i++) {
                            long startPos = i * partSize;
                            long curPartSize = (i + 1 == partCount) ? (objectSize - startPos) : partSize;
                            ;

                            UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(
                                    bucketName, s.getKey(), bucketName, destinationFileName);
                            uploadPartCopyRequest.setUploadId(uploadId);
                            uploadPartCopyRequest.setPartSize(curPartSize);
                            uploadPartCopyRequest.setBeginIndex(startPos);
                            uploadPartCopyRequest.setPartNumber(i + 1);

                            UploadPartCopyResult uploadPartCopyResult = ossClient.uploadPartCopy(uploadPartCopyRequest);
                            System.out.println("\tPart#" + uploadPartCopyResult.getPartNumber() + " done\n");
                            partETags.add(uploadPartCopyResult.getPartETag());
                        }
                        System.out.println("Completing to upload multiparts\n");
                        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                bucketName, destinationFileName, uploadId, partETags);
                        ossClient.completeMultipartUpload(completeMultipartUploadRequest);

                        ossClient.deleteObject(bucketName, s.getKey());

                    } else {
                        System.out.println("low 1G  move");
                        CopyObjectResult result = ossClient.copyObject(bucketName, s.getKey(), bucketName, destinationFileName);
                        ossClient.deleteObject(bucketName, s.getKey());
                        System.out.println("ETag: " + result.getETag() + " LastModified: " + result.getLastModified());
                    }
                    System.out.println("the key is:" + s.getKey() + " the size is:" + objectMeta.getSize());
                }

            }
        } catch (OSSException oe) {
            oe.printStackTrace();
        } catch (ClientException ce) {
            ce.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ossClient.shutdown();
        }
        logger.info("wechatLogMove Completed");

    }

}


