package org.sharetrace.contactmatching;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.sharetrace.lambda.common.util.HandlerUtil;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.model.vertex.FactorVertex;

/**
 * A Lambda function handler that runs the {@link ContactMatchingComputation} upon an S3 PUT event
 * which indicates that new geohash data has been uploaded to S3 and ready to be transformed into
 * {@link Contact}s.
 */
public class ContactMatchingWorker implements RequestHandler<List<LocationHistory>, String> {

  // Logging messages
  private static final String FAILED_TO_SERIALIZE_MSG = HandlerUtil.getFailedToSerializeMsg();

  private static final String LOCATION_PREFIX = "location-";
  private static final String FILE_FORMAT = ".txt";
  private static final String DESTINATION_BUCKET = "sharetrace-input";

  private static final AmazonS3 S3 = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  private static final ContactMatchingComputation COMPUTATION = new ContactMatchingComputation();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  @Override
  public String handleRequest(List<LocationHistory> input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    LambdaLogger logger = context.getLogger();
    File file = createRandomTextFile();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      for (Contact contact : COMPUTATION.compute(input)) {
        FactorVertex vertex = FactorVertex.builder()
            .vertexId(IdGroup.builder()
                .addIds(contact.getFirstUser(), contact.getSecondUser())
                .build())
            .vertexValue(contact)
            .build();
        writer.write(MAPPER.writeValueAsString(vertex));
        writer.newLine();
      }
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, FAILED_TO_SERIALIZE_MSG);
    }
    S3.putObject(DESTINATION_BUCKET, file.getName(), file);
    return HandlerUtil.get200Ok();
  }

  private File createRandomTextFile() {
    return HandlerUtil.createRandomFile(LOCATION_PREFIX, FILE_FORMAT);
  }
}
