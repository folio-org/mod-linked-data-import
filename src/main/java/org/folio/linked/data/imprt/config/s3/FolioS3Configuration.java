package org.folio.linked.data.imprt.config.s3;

import lombok.RequiredArgsConstructor;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class FolioS3Configuration {
  private final FolioS3Properties folioS3Properties;

  @Bean
  public FolioS3Client folioS3Client() {
    return S3ClientFactory.getS3Client(S3ClientProperties.builder()
      .endpoint(folioS3Properties.getEndpoint())
      .secretKey(folioS3Properties.getSecretKey())
      .accessKey(folioS3Properties.getAccessKey())
      .bucket(folioS3Properties.getBucket())
      .awsSdk(folioS3Properties.isAwsSdk())
      .region(folioS3Properties.getRegion())
      .build());
  }
}
