package org.folio.linked.data.imprt.model.entity;

import static jakarta.persistence.GenerationType.SEQUENCE;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.SequenceGenerator;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Entity
@EqualsAndHashCode(exclude = {"id", "failedRdfLine", "failedMappedResource"})
@Accessors(chain = true)
public class FailedRdfLine {
  private static final String FAILED_RDF_LINE_SEQ_GEN = "failed_rdf_line_seq";

  @Id
  @Column(nullable = false)
  @SequenceGenerator(name = FAILED_RDF_LINE_SEQ_GEN, allocationSize = 1)
  @GeneratedValue(strategy = SEQUENCE, generator = FAILED_RDF_LINE_SEQ_GEN)
  private Long id;
  @ManyToOne
  @ToString.Exclude
  @JoinColumn(name = "import_result_event_id")
  private ImportResultEvent importResultEvent;
  private Long jobExecutionId;
  private Long lineNumber;
  private String description;
  private String failedRdfLine;
  private String failedMappedResource;

}
