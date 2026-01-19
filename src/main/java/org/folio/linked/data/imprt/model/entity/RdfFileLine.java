package org.folio.linked.data.imprt.model.entity;

import static jakarta.persistence.GenerationType.SEQUENCE;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Entity
@Accessors(chain = true)
public class RdfFileLine {
  private static final String RDF_FILE_LINE_SEQ_GEN = "rdf_file_line_seq";

  @Id
  @Column(nullable = false)
  @SequenceGenerator(name = RDF_FILE_LINE_SEQ_GEN, allocationSize = 1)
  @GeneratedValue(strategy = SEQUENCE, generator = RDF_FILE_LINE_SEQ_GEN)
  private Long id;

  @Column(nullable = false)
  private Long jobExecutionId;

  @Column(nullable = false)
  private Long lineNumber;

  @Column(nullable = false, columnDefinition = "TEXT")
  private String content;
}

