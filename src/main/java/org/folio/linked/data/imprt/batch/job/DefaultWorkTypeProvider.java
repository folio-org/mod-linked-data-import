package org.folio.linked.data.imprt.batch.job;

import static java.util.Objects.isNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.folio.ld.dictionary.ResourceTypeDictionary.BOOKS;
import static org.folio.ld.dictionary.ResourceTypeDictionary.CONTINUING_RESOURCES;
import static org.folio.linked.data.imprt.batch.job.Parameters.DEFAULT_WORK_TYPE;
import static org.folio.linked.data.imprt.domain.dto.DefaultWorkType.MONOGRAPH;

import java.util.Optional;
import java.util.function.Supplier;
import org.folio.ld.dictionary.ResourceTypeDictionary;
import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@StepScope
@Component
public class DefaultWorkTypeProvider implements Supplier<Optional<ResourceTypeDictionary>> {

  private final DefaultWorkType defaultWorkType;

  public DefaultWorkTypeProvider(@Value("#{jobParameters['" + DEFAULT_WORK_TYPE + "']}") DefaultWorkType type) {
    this.defaultWorkType = type;
  }

  @Override
  public Optional<ResourceTypeDictionary> get() {
    if (isNull(defaultWorkType)) {
      return empty();
    }
    return of(defaultWorkType == MONOGRAPH ? BOOKS : CONTINUING_RESOURCES);
  }
}
