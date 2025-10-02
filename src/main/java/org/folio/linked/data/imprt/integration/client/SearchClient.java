package org.folio.linked.data.imprt.integration.client;


import org.folio.linked.data.imprt.domain.dto.AuthoritySearchResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "search")
public interface SearchClient {

  @GetMapping("/authorities")
  ResponseEntity<AuthoritySearchResponse> searchAuthorities(@RequestParam("query") String query);
}
