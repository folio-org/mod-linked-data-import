package org.folio.linked.data.imprt.model;

import static jakarta.persistence.GenerationType.SEQUENCE;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Entity
@EqualsAndHashCode(of = "id")
@Accessors(chain = true)
public class FailedRdfLine {

  @Id
  @GeneratedValue(strategy = SEQUENCE, generator = "failed_rdf_line_seq")
  @SequenceGenerator(name = "failed_rdf_line_seq_gen", sequenceName = "failed_rdf_line_seq")
  private Long id;
  private Long jobInstanceId;
  private String failedRdfLine;
  private String exception;

}
