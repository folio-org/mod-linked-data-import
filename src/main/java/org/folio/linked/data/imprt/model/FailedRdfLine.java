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
  private static final String FAILED_RDF_LINE_SEQ_GEN = "failed_rdf_line_seq_gen";

  @Id
  @GeneratedValue(strategy = SEQUENCE, generator = FAILED_RDF_LINE_SEQ_GEN)
  @SequenceGenerator(name = FAILED_RDF_LINE_SEQ_GEN, sequenceName = "failed_rdf_line_seq", allocationSize = 100)
  private Long id;
  private Long jobInstanceId;
  private String failedRdfLine;
  private String exception;

}
