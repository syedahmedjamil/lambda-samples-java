package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Handler implements RequestHandler<S3Event, String> {
    private static final Logger logger = LoggerFactory.getLogger(Handler.class);
    private static final String tableName = "ajcs-learn-dynamodb-books";

    @Override
    public String handleRequest(S3Event s3event, Context context) {

        S3EventNotificationRecord record = s3event.getRecords().get(0);
        Region region = Region.of(record.getAwsRegion());
        String bucketName = record.getS3().getBucket().getName();
        String objectName = record.getS3().getObject().getUrlDecodedKey();

        try (S3Client s3 = S3Client.builder().region(region).build(); DynamoDbClient ddb = DynamoDbClient.create()) {

            //for logging
            HeadObjectResponse headObject = getHeadObject(s3, bucketName, objectName);
            logger.info("SUCCESS: retrieved " + bucketName + "/" + objectName + " of type " + headObject.contentType());

            //insert S3 object to dynamodb
            var content = getContentString(s3, bucketName, objectName);
            var writeRequests = parseContentString(content);
            writeToDynamoDb(ddb, tableName, writeRequests);

            //for logging
            System.out.println(content);

            return "Ok";
        } catch (Exception e) {
            logger.error("ERROR: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private HeadObjectResponse getHeadObject(S3Client s3Client, String bucket, String key) {
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        return s3Client.headObject(headObjectRequest);
    }

    private static String getContentString(S3Client s3, String bucketName, String objectName) {
        GetObjectRequest request = GetObjectRequest.builder().key(objectName).bucket(bucketName).build();
        ResponseBytes<GetObjectResponse> response = s3.getObjectAsBytes(request);
        byte[] data = response.asByteArray();
        String content = new String(data);
        return content;
    }

    private static List<WriteRequest> parseContentString(String content) {

        List<WriteRequest> writeRequests = new ArrayList<>();
        JSONArray books = new JSONObject(content).getJSONArray("books");

        for (int i = 0; i < books.length(); i++) {
            JSONObject book = books.getJSONObject(i);
            HashMap<String, AttributeValue> item = new HashMap<>();

            item.put("isbn", AttributeValue.builder().s(book.getString("isbn")).build());
            item.put("title", AttributeValue.builder().s(book.getString("title")).build());
            item.put("subtitle", AttributeValue.builder().s(book.getString("subtitle")).build());
            item.put("author", AttributeValue.builder().s(book.getString("author")).build());
            item.put("published", AttributeValue.builder().s(book.getString("published")).build());
            item.put("pages", AttributeValue.builder().n(Integer.toString(book.getInt("pages"))).build());
            item.put("description", AttributeValue.builder().s(book.getString("description")).build());
            item.put("website", AttributeValue.builder().s(book.getString("website")).build());

            PutRequest putRequest = PutRequest.builder().item(item).build();
            WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
            writeRequests.add(writeRequest);
        }
        return writeRequests;
    }

    private static void writeToDynamoDb(DynamoDbClient ddb, String tableName, List<WriteRequest> writeRequests) {

        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(Map.of(tableName, writeRequests))
                .build();
        try {
            BatchWriteItemResponse batchWriteItemResponse = ddb.batchWriteItem(batchWriteItemRequest);
            System.out.println(tableName + " was successfully updated. The request id is " + batchWriteItemResponse.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }
}