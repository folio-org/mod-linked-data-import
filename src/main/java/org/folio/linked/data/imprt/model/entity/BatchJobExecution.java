package org.folio.linked.data.imprt.model.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

@Data
@Entity
@Table(name = "batch_job_execution")
public class BatchJobExecution {

  @Id
  private Long jobExecutionId;
  private Long jobInstanceId;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  @Enumerated(value = EnumType.STRING)
  private BatchStatus status;
}

