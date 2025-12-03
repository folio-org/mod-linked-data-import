package org.folio.linked.data.imprt.model;

import static jakarta.persistence.GenerationType.SEQUENCE;

import jakarta.persistence.Column;
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
  private static final String FAILED_RDF_LINE_SEQ_GEN = "failed_rdf_line_seq";

  @Id
  @Column(nullable = false)
  @SequenceGenerator(name = FAILED_RDF_LINE_SEQ_GEN, allocationSize = 1)
  @GeneratedValue(strategy = SEQUENCE, generator = FAILED_RDF_LINE_SEQ_GEN)
  private Long id;
  private Long jobInstanceId;
  private Long lineNumber;
  private String failedRdfLine;
  private String description;

}
