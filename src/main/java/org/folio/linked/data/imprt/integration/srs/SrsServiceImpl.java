package org.folio.linked.data.imprt.integration.srs;

import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.integration.client.SrsClient;
import org.folio.marc4ld.service.marc2ld.authority.MarcAuthority2ldMapper;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class SrsServiceImpl implements SrsService {
  private final SrsClient srsClient;
  private final ObjectMapper objectMapper;
  private final MarcAuthority2ldMapper marcAuthority2ldMapper;

  @Override
  public Optional<Resource> fetchAuthorityFromSrs(String inventoryId) {
    return ofNullable(srsClient.getAuthorityByInventoryId(inventoryId))
      .flatMap(this::contentAsJsonString)
      .flatMap(this::firstAuthorityToEntity);
  }

  private Optional<String> contentAsJsonString(ResponseEntity<Record> response) {
    return ofNullable(response.getBody())
      .map(Record::getParsedRecord)
      .map(ParsedRecord::getContent)
      .map(content -> {
        try {
          return objectMapper.writeValueAsString(content);
        } catch (JsonProcessingException e) {
          log.error("Error converting SRS content to JSON string: {}", content, e);
          return null;
        }
      });
  }

  private Optional<Resource> firstAuthorityToEntity(String marcJson) {
    return ofNullable(marcJson)
      .map(marcAuthority2ldMapper::fromMarcJson)
      .flatMap(resources -> resources.stream().findFirst());
  }
}
