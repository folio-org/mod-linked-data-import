package org.folio.linked.data.imprt.model.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "batch_step_execution")
public class BatchStepExecution {

  @Id
  @Column(name = "step_execution_id")
  private Long stepExecutionId;

  @Column(name = "job_execution_id")
  private Long jobExecutionId;

  @Column(name = "read_count")
  private Long readCount;
}

