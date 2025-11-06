package org.folio.linked.data.imprt.integration.client;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

import java.util.Set;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.SearchResourcesRequestDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;

@FeignClient(name = "linked-data")
public interface LinkedDataClient {

  @PostMapping(value = "/resources/search", produces = APPLICATION_JSON_VALUE)
  ResponseEntity<Set<Resource>> searchResources(SearchResourcesRequestDto searchRequest);

}
