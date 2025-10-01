package org.folio.linked.data.imprt.integration.client;

import org.folio.rest.jaxrs.model.Record;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "source-storage")
public interface SrsClient {

  @GetMapping(value = "/records/{inventoryId}/formatted?idType=AUTHORITY")
  ResponseEntity<Record> getAuthorityByInventoryId(@PathVariable("inventoryId") String inventoryId);
}
