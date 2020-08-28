package org.sharetrace.contactmatching;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.util.ShareTraceUtil;

/*
TODO Add the functionality to take MULTIPLE pairs of unqiue entries to handle per worker payload.
 This allows the flexibility of "combining" many small files written earlier by the ReadWorker
 into fewer, larger files.

 TODO For "merging" contact data, do not do it in the Lambda handler. Set an object lifecycle
  policy on all objects prefixed with "locations". Similarly with risk scores.
 */

/**
 * A Lambda function handler that runs the {@link ContactMatchingComputation} upon an S3 PUT event
 * which indicates that new geohash data has been uploaded to S3 and ready to be transformed into
 * {@link Contact}s.
 */
public class ContactMatchingHandler implements RequestHandler<S3Event, String> {

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  private static final AmazonS3 S3 = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  private static final long INIT_TIMESTAMP = System.currentTimeMillis();

  private static final String OUTPUT_FILE = String.format("out/factor/%d.txt", INIT_TIMESTAMP);

  private static final String KEY_NAME = Paths.get(OUTPUT_FILE).getFileName().toString();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final ContactMatchingComputation COMPUTATION = new ContactMatchingComputation();

  @Override
  public String handleRequest(S3Event input, Context context) {
    log(input, context);
    LambdaLogger logger = context.getLogger();
    String bucketName = getBucketName(input);
    List<S3ObjectSummary> objects = S3.listObjectsV2(bucketName).getObjectSummaries();
    String uploadId = S3
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, KEY_NAME))
        .getUploadId();
    List<PartETag> tags = new ArrayList<>();
    int nPartitions = objects.size();
    int iPartition = 0;
    Set<Map.Entry<Integer, Integer>> uniqueEntries = COMPUTATION.getUniqueEntries(nPartitions);
    if (uniqueEntries.isEmpty()) {
      uniqueEntries = ImmutableSet.of(new SimpleImmutableEntry<>(0, 0));
    }
    for (Map.Entry<Integer, Integer> entry : uniqueEntries) {
      S3Object object = S3.getObject(bucketName, objects.get(entry.getKey()).getKey());
      S3Object otherObject = S3.getObject(bucketName, objects.get(entry.getValue()).getKey());
      List<LocationHistory> allLocations = mapObjects(object, otherObject, logger);

      try {
        File partFile = writeFile(allLocations);
        PartETag eTag = uploadPart(bucketName, uploadId, iPartition, partFile);
        tags.add(eTag);
      } catch (IOException e) {
        logger.log(e.getMessage());
        logger.log(Arrays.toString(e.getStackTrace()));
      }

      iPartition++;
    }

    S3.completeMultipartUpload(
        new CompleteMultipartUploadRequest(bucketName, KEY_NAME, uploadId, tags));
    return null;
  }

  private void log(S3Event input, Context context) {
    LambdaLogger logger = context.getLogger();
    try {
      String environmentVariablesLog = STRING_BUILDER
          .append(ENVIRONMENT_VARIABLES)
          .append(MAPPER.writeValueAsString(System.getenv()))
          .toString();
      logger.log(environmentVariablesLog);
      resetStringBuilder();

      String contextLog = STRING_BUILDER
          .append(CONTEXT)
          .append(MAPPER.writeValueAsString(context))
          .toString();
      logger.log(contextLog);
      resetStringBuilder();

      String eventLog = STRING_BUILDER
          .append(EVENT)
          .append(MAPPER.writeValueAsString(input))
          .toString();
      logger.log(eventLog);
      resetStringBuilder();

      String eventTypeLog = STRING_BUILDER
          .append(EVENT_TYPE)
          .append(input.getClass().getSimpleName())
          .toString();
      logger.log(eventTypeLog);
      resetStringBuilder();

    } catch (JsonProcessingException e) {
      logger.log(e.getMessage());
      logger.log(Arrays.toString(e.getStackTrace()));
    }
  }

  private void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }

  private String getBucketName(S3Event input) {
    S3EventNotificationRecord record = input.getRecords().get(0);
    return record.getS3().getBucket().getName();
  }

  private List<LocationHistory> mapObjects(S3Object o1, S3Object o2, LambdaLogger logger) {
    Set<LocationHistory> mapping = loadPartition(o1, logger);
    Set<LocationHistory> otherMapping = loadPartition(o2, logger);
    Set<LocationHistory> allLocations = new HashSet<>();
    allLocations.addAll(mapping);
    allLocations.addAll(otherMapping);
    return ImmutableList.copyOf(allLocations);
  }

  private Set<LocationHistory> loadPartition(S3Object object, LambdaLogger logger) {
    S3ObjectInputStream inputStream = object.getObjectContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
    return reader.lines()
        .map(line -> {
          LocationHistory mapped = null;
          try {
            mapped = MAPPER.readValue(line, LocationHistory.class);
          } catch (JsonProcessingException e) {
            logger.log(e.getMessage());
            logger.log(Arrays.toString(e.getStackTrace()));
          }
          return mapped;
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  private File writeFile(List<LocationHistory> computeInput) throws IOException {
    File file = new File(KEY_NAME);
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    for (Contact contact : COMPUTATION.compute(computeInput)) {
      writer.write(MAPPER.writeValueAsString(contact));
      writer.newLine();
    }
    writer.close();
    return file;
  }

  private PartETag uploadPart(String bucketName, String uploadId, int part, File file)
      throws IOException {
    UploadPartRequest uploadRequest = new UploadPartRequest()
        .withBucketName(bucketName)
        .withKey(KEY_NAME)
        .withPartNumber(part)
        .withUploadId(uploadId)
        .withFile(file);
    return S3.uploadPart(uploadRequest).getPartETag();
  }
}
