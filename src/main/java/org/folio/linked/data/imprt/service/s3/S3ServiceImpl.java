package org.folio.linked.data.imprt.service.s3;

import org.springframework.stereotype.Service;

@Service
public class S3ServiceImpl implements S3Service {

  @Override
  public boolean exists(String fileUrl) {
    return true;
  }

}
