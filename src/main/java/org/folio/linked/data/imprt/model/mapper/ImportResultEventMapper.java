package org.folio.linked.data.imprt.model.mapper;

import java.util.Set;
import org.folio.linked.data.imprt.domain.dto.FailedResource;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface ImportResultEventMapper {

  @Mapping(target = "id", ignore = true)
  @Mapping(target = "eventTs", source = "ts")
  @Mapping(target = "failedRdfLines", expression = "java(mapFailedResources(dto, importResultEvent))")
  ImportResultEvent toEntity(org.folio.linked.data.imprt.domain.dto.ImportResultEvent dto);

  @Mapping(target = "id", ignore = true)
  @Mapping(target = "jobInstanceId", source = "jobInstanceId")
  @Mapping(target = "failedRdfLine", ignore = true)
  @Mapping(target = "importResultEvent", ignore = true)
  @Mapping(target = "failedMappedResource", source = "failedResource.resource")
  FailedRdfLine toFailedRdfLine(FailedResource failedResource, Long jobInstanceId);

  default Set<FailedRdfLine> mapFailedResources(org.folio.linked.data.imprt.domain.dto.ImportResultEvent dto,
                                                ImportResultEvent entity) {
    if (dto == null || dto.getFailedResources() == null) {
      return null;
    }
    return dto.getFailedResources().stream()
      .map(fr -> toFailedRdfLine(fr, dto.getJobInstanceId()))
      .map(fr -> fr.setImportResultEvent(entity))
      .collect(java.util.stream.Collectors.toSet());
  }

}

