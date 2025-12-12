package org.folio.linked.data.imprt.model.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "batch_step_execution")
public class BatchStepExecution {

  @Id
  private Long stepExecutionId;
  private Long jobExecutionId;
  private Long readCount;
}

