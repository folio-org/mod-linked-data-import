package org.folio.linked.data.imprt.integration.search;

import static java.util.Optional.ofNullable;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.AuthorityItem;
import org.folio.linked.data.imprt.domain.dto.AuthoritySearchResponse;
import org.folio.linked.data.imprt.integration.client.SearchClient;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SearchServiceImpl implements SearchService {
  private final SearchClient searchClient;

  @Override
  public Optional<String> searchForInventoryId(String lccn) {
    return ofNullable(searchClient.searchAuthorities("lccn = " + lccn)
      .getBody())
      .map(AuthoritySearchResponse::getAuthorities)
      .stream()
      .flatMap(Collection::stream)
      .map(AuthorityItem::getId)
      .filter(Objects::nonNull)
      .findFirst();
  }
}
