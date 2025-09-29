package org.folio.linked.data.imprt.config;

import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FolioS3Configuration {
  @Value("${mod-linked-data-import.remote-files-storage.endpoint}")
  private String endpoint;

  @Value("${mod-linked-data-import.remote-files-storage.accessKey}")
  private String accessKey;

  @Value("${mod-linked-data-import.remote-files-storage.secretKey}")
  private String secretKey;

  @Value("${mod-linked-data-import.remote-files-storage.bucket}")
  private String bucket;

  @Value("${mod-linked-data-import.remote-files-storage.region}")
  private String region;

  @Value("#{T(Boolean).parseBoolean('${mod-linked-data-import.remote-files-storage.awsSdk}')}")
  private boolean awsSdk;

  @Bean
  public FolioS3Client folioS3Client() {
    return S3ClientFactory.getS3Client(S3ClientProperties.builder()
      .endpoint(endpoint)
      .secretKey(secretKey)
      .accessKey(accessKey)
      .bucket(bucket)
      .awsSdk(awsSdk)
      .region(region)
      .build());
  }
}
