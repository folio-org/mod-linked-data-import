package org.folio.linked.data.imprt.util;

import static java.util.Optional.ofNullable;
import static org.folio.spring.integration.XOkapiHeaders.TENANT;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.Logger;
import org.folio.linked.data.imprt.service.tenant.LinkedDataImportTenantService;

@UtilityClass
public class KafkaUtils {

  public static Optional<String> getHeaderValueByName(ConsumerRecord<String, ?> consumerRecord, String headerName) {
    return ofNullable(consumerRecord.headers().lastHeader(headerName))
      .map(header -> new String(header.value()));
  }

  public static <T> void handleForExistedTenant(ConsumerRecord<String, T> consumerRecord,
                                                String eventId,
                                                LinkedDataImportTenantService tenantService,
                                                Logger log,
                                                Consumer<ConsumerRecord<String, T>> handler) {
    var event = consumerRecord.value();
    var eventName = event.getClass().getSimpleName();
    var tenant = getHeaderValueByName(consumerRecord, TENANT)
      .orElseThrow(() -> new IllegalArgumentException("Received %s [id %s] is missing x-okapi-tenant header"
        .formatted(eventName, eventId)));
    boolean tenantExists = tenantService.isTenantExists(tenant);
    if (tenantExists) {
      handler.accept(consumerRecord);
    } else {
      log.debug("Received {} [id {}] will be ignored since module is not installed on tenant {}",
        eventName, eventId, tenant);
    }
  }
}
