package org.folio.linked.data.imprt.model.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Entity
@IdClass(BatchJobExecutionParams.BatchJobExecutionParamsId.class)
@EqualsAndHashCode(of = {"jobExecutionId", "parameterName"})
public class BatchJobExecutionParams {

  @Id
  private Long jobExecutionId;
  @Id
  private String parameterName;
  private String parameterValue;

  @Data
  public static class BatchJobExecutionParamsId implements Serializable {
    private Long jobExecutionId;
    private String parameterName;
  }
}

