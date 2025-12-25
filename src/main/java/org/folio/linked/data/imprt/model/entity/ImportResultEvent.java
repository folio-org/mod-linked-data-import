package org.folio.linked.data.imprt.model.entity;

import static jakarta.persistence.GenerationType.SEQUENCE;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.SequenceGenerator;
import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Entity
@EqualsAndHashCode(of = "id")
@Accessors(chain = true)
public class ImportResultEvent {
  private static final String IMPORT_RESULT_EVENT_SEQ_GEN = "import_result_event_seq";

  @Id
  @Column(nullable = false)
  @SequenceGenerator(name = IMPORT_RESULT_EVENT_SEQ_GEN, allocationSize = 1)
  @GeneratedValue(strategy = SEQUENCE, generator = IMPORT_RESULT_EVENT_SEQ_GEN)
  private Long id;
  private Long jobExecutionId;
  private int resourcesCount;
  private int createdCount;
  private int updatedCount;
  private OffsetDateTime startDate;
  private OffsetDateTime endDate;
  private String originalEventTs;
  private String eventTs;

  @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "importResultEvent")
  private Set<FailedRdfLine> failedRdfLines = new LinkedHashSet<>();

}
