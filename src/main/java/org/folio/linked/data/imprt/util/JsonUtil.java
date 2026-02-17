package org.folio.linked.data.imprt.util;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

import lombok.experimental.UtilityClass;
import tools.jackson.databind.json.JsonMapper;

@UtilityClass
public class JsonUtil {

  public static final JsonMapper JSON_MAPPER = JsonMapper.builder()
    .changeDefaultPropertyInclusion(incl -> incl.withValueInclusion(NON_EMPTY))
    .configure(tools.jackson.databind.SerializationFeature.USE_EQUALITY_FOR_OBJECT_ID, true)
    .configure(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .configure(tools.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
    .build();
}
