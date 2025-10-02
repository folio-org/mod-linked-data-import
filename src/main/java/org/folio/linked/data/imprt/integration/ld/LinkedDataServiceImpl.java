package org.folio.linked.data.imprt.integration.ld;

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.SearchResourcesRequestDto;
import org.folio.linked.data.imprt.integration.client.LinkedDataClient;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class LinkedDataServiceImpl implements LinkedDataService {

  private final ObjectMapper objectMapper;
  private final LinkedDataClient linkedDataClient;

  @Override
  public Set<Resource> searchResources(Set<String> inventoryIds) {
    return ofNullable(linkedDataClient.searchResources(new SearchResourcesRequestDto(inventoryIds))
      .getBody())
      .stream()
      .flatMap(Collection::stream)
      .map(this::readJsonResource)
      .flatMap(Optional::stream)
      .collect(Collectors.toSet());
  }

  private Optional<Resource> readJsonResource(String json) {
    try {
      return Optional.of(objectMapper.readValue(json, Resource.class));
    } catch (JsonProcessingException e) {
      log.error("Error reading Resource from JSON: {}", json, e);
      return Optional.empty();
    }
  }

}
