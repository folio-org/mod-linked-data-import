package org.folio.linked.data.imprt.config.s3;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("folio.remote-storage")
public class FolioS3Properties {

  private String endpoint;
  private String accessKey;
  private String secretKey;
  private String bucket;
  private String region;
  private boolean awsSdk;

}
