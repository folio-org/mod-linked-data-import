package org.folio.linked.data.imprt.model.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.LinkedHashSet;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

@UnitTest
class ImportResultEventMapperTest {

  private final ImportResultEventMapper mapper = Mappers.getMapper(ImportResultEventMapper.class);

  @Test
  void toEntity_shouldMapDtoToEntity() {
    // given
    var jobExecutionId = 123L;
    var startDate = OffsetDateTime.now();
    var endDate = OffsetDateTime.now();
    var dto = new ImportResultEvent(
      "original-ts-123",
      jobExecutionId,
      startDate,
      endDate,
      100,
      80,
      20
    );
    dto.setTs("event-ts-456");
    dto.setTenant("test_tenant");

    // when
    var entity = mapper.toEntity(dto);

    // then
    assertThat(entity).isNotNull();
    assertThat(entity.getId()).isNull();
    assertThat(entity.getJobExecutionId()).isEqualTo(jobExecutionId);
    assertThat(entity.getResourcesCount()).isEqualTo(100);
    assertThat(entity.getCreatedCount()).isEqualTo(80);
    assertThat(entity.getUpdatedCount()).isEqualTo(20);
    assertThat(entity.getStartDate()).isEqualTo(startDate);
    assertThat(entity.getEndDate()).isEqualTo(endDate);
    assertThat(entity.getOriginalEventTs()).isEqualTo("original-ts-123");
    assertThat(entity.getEventTs()).isEqualTo("event-ts-456");
    assertThat(entity.getFailedRdfLines()).isEmpty();
  }

  @Test
  void toEntity_shouldMapFailedResourcesAndSetBidirectionalRelation() {
    // given
    var jobExecutionId = 456L;
    var dto = new ImportResultEvent(
      "original-ts",
      jobExecutionId,
      OffsetDateTime.now(),
      OffsetDateTime.now(),
      10,
      8,
      2
    );
    dto.setTs("event-ts");
    dto.setTenant("test_tenant");

    var failedResource1 = new FailedResource(5L, "Error message 1");
    var failedResource2 = new FailedResource(10L, "Error message 2");
    var failedResources = new LinkedHashSet<FailedResource>();
    failedResources.add(failedResource1);
    failedResources.add(failedResource2);
    dto.setFailedResources(failedResources);

    // when
    var entity = mapper.toEntity(dto);

    // then
    assertThat(entity).isNotNull();
    assertThat(entity.getFailedRdfLines()).hasSize(2);

    var failedLines = entity.getFailedRdfLines().stream()
      .sorted(Comparator.comparingLong(org.folio.linked.data.imprt.model.entity.FailedRdfLine::getLineNumber))
      .toList();

    var failedLine1 = failedLines.getFirst();
    assertThat(failedLine1.getId()).isNull();
    assertThat(failedLine1.getLineNumber()).isEqualTo(5L);
    assertThat(failedLine1.getJobExecutionId()).isEqualTo(jobExecutionId);
    assertThat(failedLine1.getDescription()).isEqualTo("Error message 1");
    assertThat(failedLine1.getFailedRdfLine()).isNull();
    assertThat(failedLine1.getImportResultEvent()).isEqualTo(entity);

    var failedLine2 = failedLines.get(1);
    assertThat(failedLine2.getId()).isNull();
    assertThat(failedLine2.getLineNumber()).isEqualTo(10L);
    assertThat(failedLine2.getJobExecutionId()).isEqualTo(jobExecutionId);
    assertThat(failedLine2.getDescription()).isEqualTo("Error message 2");
    assertThat(failedLine2.getFailedRdfLine()).isNull();
    assertThat(failedLine2.getImportResultEvent()).isEqualTo(entity);
  }

  @Test
  void toEntity_shouldHandleNullFailedResources() {
    // given
    var dto = new ImportResultEvent(
      "original-ts",
      123L,
      OffsetDateTime.now(),
      OffsetDateTime.now(),
      10,
      10,
      0
    );
    dto.setTs("event-ts");
    dto.setFailedResources(null);

    // when
    var entity = mapper.toEntity(dto);

    // then
    assertThat(entity).isNotNull();
    assertThat(entity.getFailedRdfLines()).isNull();
  }

  @Test
  void toEntity_shouldHandleEmptyFailedResources() {
    // given
    var dto = new ImportResultEvent(
      "original-ts",
      123L,
      OffsetDateTime.now(),
      OffsetDateTime.now(),
      10,
      10,
      0
    );
    dto.setTs("event-ts");
    dto.setFailedResources(new LinkedHashSet<>());

    // when
    var entity = mapper.toEntity(dto);

    // then
    assertThat(entity).isNotNull();
    assertThat(entity.getFailedRdfLines()).isEmpty();
  }

  @Test
  void toFailedRdfLine_shouldMapFailedResourceToEntity() {
    // given
    var failedResource = new FailedResource(15L, "Description text");
    var jobExecutionId = 999L;

    // when
    var failedLine = mapper.toFailedRdfLine(failedResource, jobExecutionId);

    // then
    assertThat(failedLine).isNotNull();
    assertThat(failedLine.getId()).isNull();
    assertThat(failedLine.getLineNumber()).isEqualTo(15L);
    assertThat(failedLine.getJobExecutionId()).isEqualTo(jobExecutionId);
    assertThat(failedLine.getDescription()).isEqualTo("Description text");
    assertThat(failedLine.getFailedRdfLine()).isNull();
    assertThat(failedLine.getImportResultEvent()).isNull();
  }

}

