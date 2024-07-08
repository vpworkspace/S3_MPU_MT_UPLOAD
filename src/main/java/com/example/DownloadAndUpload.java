package com.example;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class DownloadAndUpload {
    private static final int MIN_PART_SIZE = 100 * 1024 * 1024; // 5 MB
    private static final int THREAD_COUNT = 4; // Number of threads for uploading parts

    public static void main(String[] args) {
        String fileUrl = "https://github.com/szalony9szymek/large/releases/download/free/large";
        String bucketName = "vpmpuupload";
        String keyName = "largefile2";

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(fileUrl);

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                S3Client s3Client = createS3Client();
                String uploadId = initiateMultipartUpload(s3Client, bucketName, keyName);

                try (InputStream inputStream = entity.getContent()) {
                    List<CompletedPart> completedParts = uploadParts(s3Client, inputStream, bucketName, keyName, uploadId);
                    completeMultipartUpload(s3Client, bucketName, keyName, uploadId, completedParts);
                    System.out.println("File uploaded to S3 successfully!");
                } catch (Exception e) {
                    abortMultipartUpload(s3Client, bucketName, keyName, uploadId);
                    e.printStackTrace();
                }
            }
        } catch (IOException | S3Exception e) {
            e.printStackTrace();
        }
    }

    private static S3Client createS3Client() {
        return S3Client.builder()
                .region(Region.AP_SOUTH_1) // Specify the region
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    private static String initiateMultipartUpload(S3Client s3Client, String bucketName, String keyName) {
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        return createMultipartUploadResponse.uploadId();
    }

    private static List<CompletedPart> uploadParts(S3Client s3Client, InputStream inputStream, String bucketName, String keyName, String uploadId) throws IOException, InterruptedException, ExecutionException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<CompletedPart>> futures = new ArrayList<>();
        List<CompletedPart> completedParts = new ArrayList<>();
        byte[] buffer = new byte[MIN_PART_SIZE];
        int bytesRead;
        int partNumber = 1;

        // Buffer to accumulate data until it reaches at least MIN_PART_SIZE
        ByteArrayOutputStream partBuffer = new ByteArrayOutputStream();

        while ((bytesRead = inputStream.read(buffer)) != -1) {
            partBuffer.write(buffer, 0, bytesRead);
            // If buffer size reaches at least MIN_PART_SIZE, upload the part
            if (partBuffer.size() >= MIN_PART_SIZE) {
                final int currentPartNumber = partNumber;
                final byte[] currentBuffer = partBuffer.toByteArray(); // Copy the accumulated data

                Callable<CompletedPart> task = () -> {
                    System.out.println("currentPartNumber:" + currentPartNumber);
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(keyName)
                            .uploadId(uploadId)
                            .partNumber(currentPartNumber)
                            .build();
                    UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest,
                            RequestBody.fromBytes(currentBuffer)); // Use fromBytes with the correct byte array
                    return CompletedPart.builder()
                            .partNumber(currentPartNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build();
                };

                futures.add(executorService.submit(task));
                partNumber++;
                partBuffer.reset(); // Reset the buffer for the next part
            }
        }

        // Handle the last part, if there is any remaining data in the buffer
        if (partBuffer.size() > 0) {
            final int currentPartNumber = partNumber;
            final byte[] lastBuffer = partBuffer.toByteArray(); // Copy the remaining data

            Callable<CompletedPart> task = () -> {
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .uploadId(uploadId)
                        .partNumber(currentPartNumber)
                        .build();
                UploadPartResponse uploadPartResponse = s3Client.uploadPart(uploadPartRequest,
                        RequestBody.fromBytes(lastBuffer)); // Use fromBytes with the correct byte array
                return CompletedPart.builder()
                        .partNumber(currentPartNumber)
                        .eTag(uploadPartResponse.eTag())
                        .build();
            };

            futures.add(executorService.submit(task));
        }

        // Wait for all futures to complete
        for (Future<CompletedPart> future : futures) {
            completedParts.add(future.get());
        }

        executorService.shutdown();
        return completedParts;
    }

    private static void completeMultipartUpload(S3Client s3Client, String bucketName, String keyName, String uploadId, List<CompletedPart> completedParts) {
        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .uploadId(uploadId)
                .multipartUpload(completedMultipartUpload)
                .build();
        s3Client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static void abortMultipartUpload(S3Client s3Client, String bucketName, String keyName, String uploadId) {
        s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .uploadId(uploadId)
                .build());
    }
}
