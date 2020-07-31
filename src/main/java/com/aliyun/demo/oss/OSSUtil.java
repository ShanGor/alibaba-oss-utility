/**
 * 示例说明
 * <p>
 * HelloOSS是OSS Java SDK的示例程序，您可以修改endpoint、accessKeyId、accessKeySecret、bucketName后直接运行。
 * 运行方法请参考README。
 * <p>
 * 本示例中的并不包括OSS Java SDK的所有功能，详细功能及使用方法，请参看“SDK手册 > Java-SDK”，
 * 链接地址是：https://help.aliyun.com/document_detail/oss/sdk/java-sdk/preface.html?spm=5176.docoss/sdk/java-sdk/。
 * <p>
 * 调用OSS Java SDK的方法时，抛出异常表示有错误发生；没有抛出异常表示成功执行。
 * 当错误发生时，OSS Java SDK的方法会抛出异常，异常中包括错误码、错误信息，详细请参看“SDK手册 > Java-SDK > 异常处理”，
 * 链接地址是：https://help.aliyun.com/document_detail/oss/sdk/java-sdk/exception.html?spm=5176.docoss/api-reference/error-response。
 * <p>
 * OSS控制台可以直观的看到您调用OSS Java SDK的结果，OSS控制台地址是：https://oss.console.aliyun.com/index#/。
 * OSS控制台使用方法请参看文档中心的“控制台用户指南”， 指南的来链接地址是：https://help.aliyun.com/document_detail/oss/getting-started/get-started.html?spm=5176.docoss/user_guide。
 * <p>
 * OSS的文档中心地址是：https://help.aliyun.com/document_detail/oss/user_guide/overview.html。
 * OSS Java SDK的文档地址是：https://help.aliyun.com/document_detail/oss/sdk/java-sdk/install.html?spm=5176.docoss/sdk/java-sdk。
 */

package com.aliyun.demo.oss;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.aliyun.demo.util.Pair;
import com.aliyun.demo.util.TimeKeeper;
import com.aliyun.oss.*;
import com.aliyun.oss.model.*;

import org.apache.log4j.Logger;

public class OSSUtil implements Closeable {
    private static Logger logger = Logger.getLogger(OSSUtil.class);
    private OSS ossClient = null;
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;

    public OSSUtil() {
        //Works in Unix/Linux, in Windows, you need to specify HOME as env variable
        // Do not use System.getProperty("user.home") because in Dataworks Shell, it is not equal to HOME
        String home = System.getenv("HOME");
        logger.info("Running Java with home folder: " + home);
        Properties properties = new Properties();
        try (InputStream ins = new FileInputStream(home + "/.ossutilconfig")) {
            properties.load(ins);
        } catch (IOException e) {
            logger.info(e.getMessage());
            logger.info("Failed to get ~/.ossutilconfig. Aborted!");
            return;
        }

        endpoint = properties.getProperty("endpoint");
        accessKeyId = properties.getProperty("accessKeyID");
        accessKeySecret = properties.getProperty("accessKeySecret");
        ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    public OSSUtil(String endpoint, String accessKeyId, String accessKeySecret) {
        // 生成OSSClient，您可以指定一些参数，详见“SDK手册 > Java-SDK > 初始化”，
        // 链接地址是：https://help.aliyun.com/document_detail/oss/sdk/java-sdk/init.html?spm=5176.docoss/sdk/java-sdk/get-start
        ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: java -jar oss-utility.jar ls|download|upload from-path to-path");
            return;
        }

        switch (args[0].trim()) {
            case "ls":
                String ossUri = args[1].trim();
                Pair<String, String> info = OSSUtil.getObjectInfoFromURI(ossUri);
                try (OSSUtil ossUtil = new OSSUtil()) {
                    System.out.print("===key:" + info.getKey() + " value:" + info.getValue());
                    ossUtil.list(info.getKey(), info.getValue());
                }
                break;

            /**
             * download oss://bucket/path local-path
             */
            case "download":
                ossUri = args[1].trim();
                String localPath = args[2].trim();
                info = OSSUtil.getObjectInfoFromURI(ossUri);
                try (OSSUtil ossUtil = new OSSUtil()) {
                    logger.info("Starting to download..");
                    TimeKeeper tk = new TimeKeeper();
                    ossUtil.ossClient.getObject(new GetObjectRequest(info.getKey(), info.getValue()), new File(localPath));
                    logger.info("Done the download in " + tk.elapsedSeconds() + " seconds!");
                }

                break;
            case "upload":
                localPath = args[1].trim();
                ossUri = args[2].trim();
                info = OSSUtil.getObjectInfoFromURI(ossUri);
                try (OSSUtil ossUtil = new OSSUtil()) {
                    logger.info("Starting to upload..");
                    TimeKeeper tk = new TimeKeeper();
                    ossUtil.uploadLarge(info.getKey(), info.getValue(), localPath);
                    logger.info("Done the upload in " + tk.elapsedSeconds() + " seconds!");
                }

                break;
            case "mv":
                String sourcePath = args[1].trim();
                String targetPath = args[2].trim();
                Pair<String, String> sourceInfo = OSSUtil.getObjectInfoFromURI(sourcePath);
                Pair<String, String> targetInfo = OSSUtil.getObjectInfoFromURI(targetPath);
                try (OSSUtil ossUtil = new OSSUtil()) {
                    logger.info("Starting to move..");
                    TimeKeeper tk = new TimeKeeper();
                    ossUtil.moveFiles(sourceInfo.getKey(), sourceInfo.getValue(), targetInfo.getValue());
                    logger.info("Done the move in " + tk.elapsedSeconds() + " seconds!");
                }

                break;
            default:
                System.out.println("Unknown action: " + args[0]);
        }

    }

    /**
     * Only support encoding as UTF-8. If file name ends with '.gz', will try to decompress it.
     * The path does not start with '/'.
     *
     * @param bucketName
     * @param path
     * @return
     * @throws IOException
     */
    public BufferedReader getBufferReader(String bucketName, String path) throws IOException {
        return new BufferedReader(new InputStreamReader(getStream(bucketName, path), StandardCharsets.UTF_8));
    }

    public InputStream getStream(String bucketName, String path) throws IOException {
        logger.info("Preparing oss://" + bucketName + "/" + path + " as an input stream..");
        InputStream objIns = ossClient.getObject(bucketName, path).getObjectContent();
        InputStream decoratedInputStream = objIns;
        if (path.toLowerCase(Locale.ROOT).endsWith(".gz")) {
            decoratedInputStream = new GZIPInputStream(objIns);
        }
        return decoratedInputStream;
    }

    /**
     * ls path
     *
     * @param bucketName: 命名规范如下：只能包括小写字母，数字和短横线（-），必须以小写字母或者数字开头，长度必须在3-63字节之间。
     * @param path:       命名规范如下：使用UTF-8编码，长度必须在1-1023字节之间，不能以“/”或者“\”字符开头。
     */
    public void list(String bucketName, String path) {
        if (!ossClient.doesBucketExist(bucketName)) {
            System.out.println("Error: Your Bucket does not exist: " + bucketName);
            return;
        }
        ossClient.listObjects(bucketName, path).getObjectSummaries()
                .forEach(o -> System.out.println(o.getKey()));
    }

    @Override
    public void close() throws IOException {
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

    public OSS getOssClient() {
        return ossClient;
    }

    public void upload(String bucketName, String uploadToPath, String localPath) throws IOException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("Content-MD5", md5sumBase64(localPath));

        ossClient.putObject(bucketName, uploadToPath, new File(localPath), metadata);
    }

    public void uploadLarge(String bucketName, String uploadToPath, String localPath) throws IOException {
        // 创建InitiateMultipartUploadRequest对象。
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, uploadToPath);

        // 初始化分片。
        InitiateMultipartUploadResult upresult = ossClient.initiateMultipartUpload(request);
        // 返回uploadId，它是分片上传事件的唯一标识，您可以根据这个uploadId发起相关的操作，如取消分片上传、查询分片上传等。
        String uploadId = upresult.getUploadId();

        // partETags是PartETag的集合。PartETag由分片的ETag和分片号组成。
        List<PartETag> partETags = new ArrayList<PartETag>();
        // 计算文件有多少个分片。
        final long partSize = 1 * 1024 * 1024L;   // 1MB
        final File sampleFile = new File(localPath);
        long fileLength = sampleFile.length();
        int partCount = (int) (fileLength / partSize);
        if (fileLength % partSize != 0) {
            partCount++;
        }

        // 遍历分片上传。
        for (int i = 0; i < partCount; i++) {
            long startPos = i * partSize;
            long curPartSize = (i + 1 == partCount) ? (fileLength - startPos) : partSize;
            InputStream instream = new FileInputStream(sampleFile);
            // 跳过已经上传的分片。
            instream.skip(startPos);
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucketName);
            uploadPartRequest.setKey(uploadToPath);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setInputStream(instream);
            // 设置分片大小。除了最后一个分片没有大小限制，其他的分片最小为100 KB。
            uploadPartRequest.setPartSize(curPartSize);
            // 设置分片号。每一个上传的分片都有一个分片号，取值范围是1~10000，如果超出这个范围，OSS将返回InvalidArgument的错误码。
            uploadPartRequest.setPartNumber(i + 1);
            // 每个分片不需要按顺序上传，甚至可以在不同客户端上传，OSS会按照分片号排序组成完整的文件。
            UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
            // 每次上传分片之后，OSS的返回结果包含PartETag。PartETag将被保存在partETags中。
            partETags.add(uploadPartResult.getPartETag());
        }

        // 创建CompleteMultipartUploadRequest对象。
        // 在执行完成分片上传操作时，需要提供所有有效的partETags。OSS收到提交的partETags后，会逐一验证每个分片的有效性。当所有的数据分片验证通过后，OSS将把这些分片组合成一个完整的文件。
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, uploadToPath, uploadId, partETags);

        // 如果需要在完成文件上传的同时设置文件访问权限，请参考以下示例代码。
        // completeMultipartUploadRequest.setObjectACL(CannedAccessControlList.PublicRead);

        // 完成上传。
        CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);

        logger.info("ETag is: " + completeMultipartUploadResult.getETag());
    }

    public static final String md5sumBase64(final String localPath) throws IOException {
        return Base64.getEncoder().encodeToString(org.apache.commons.codec.digest.DigestUtils.md5(new FileInputStream(localPath)));
    }

    /**
     * Given a URI like oss://bucket-name/path/subpath, split it and return (bucket-name, path/subpath)
     *
     * @param ossURI
     * @return
     */
    public static final Pair<String, String> getObjectInfoFromURI(final String ossURI) throws MalformedURLException {
        if (ossURI.startsWith("oss://")) {
            String t1 = ossURI.substring("oss://".length());
            int t2 = t1.indexOf('/');
            String bucketName = t1.substring(0, t2);
            String pathOnOss = t1.substring(t2 + 1);

            return new Pair<>(bucketName, pathOnOss);
        } else {
            throw new MalformedURLException("Unknown OSS URI: " + ossURI + ", expect to starts with oss://");
        }
    }

    public void moveFiles(String bucketName, String sourcePath, String targetPath) throws IOException {
        ObjectListing objectListing = ossClient.listObjects(new ListObjectsRequest(bucketName).withMarker(sourcePath));
        List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
        for (OSSObjectSummary s : sums) {
            if (s.getKey().contains(sourcePath)) {
                String fileName = s.getKey().substring(s.getKey().lastIndexOf("/") + 1);
                String destinationFileName = targetPath + fileName;
                copyFile(bucketName, s.getKey(), destinationFileName);
                boolean copyCheckFlag = ossClient.doesObjectExist(bucketName, destinationFileName);
                if (copyCheckFlag) {
                    deleteFile(bucketName, s.getKey());
                }
            }
        }
    }

    public void copyFile(String bucketName, String sourceFileName, String destinationFileName) {
        SimplifiedObjectMeta objectMeta = ossClient.getSimplifiedObjectMeta(bucketName, sourceFileName);

        if (objectMeta.getSize() > 1024 * 1024 * 1024) {
            InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, destinationFileName);
            InitiateMultipartUploadResult initiateMultipartUploadResult = ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
            String uploadId = initiateMultipartUploadResult.getUploadId();

            final long partSize = 1000 * 1024 * 1024L;   // partSize 1000MB
            ObjectMetadata metadata = ossClient.getObjectMetadata(bucketName, sourceFileName);
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

                UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(
                        bucketName, sourceFileName, bucketName, destinationFileName);
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
        } else {
            CopyObjectResult result = ossClient.copyObject(bucketName, sourceFileName, bucketName, destinationFileName);
            // System.out.println("ETag: " + result.getETag() + " LastModified: " + result.getLastModified());
        }
        System.out.println("the file is:" + sourceFileName + " the size is:" + objectMeta.getSize() + " bytes");
    }

    public void deleteFile(String bucketName, String fileName) {
        ossClient.deleteObject(bucketName, fileName);
    }

}
