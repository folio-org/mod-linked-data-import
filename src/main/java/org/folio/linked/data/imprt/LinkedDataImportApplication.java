package org.folio.linked.data.imprt;

import static org.springframework.context.annotation.ComponentScan.Filter;
import static org.springframework.context.annotation.FilterType.REGEX;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@EnableFeignClients
@SpringBootApplication(exclude = BatchAutoConfiguration.class)
@ComponentScan(value = "org.folio",
  excludeFilters = @Filter(type = REGEX, pattern = {"org.folio.spring.tools.systemuser.*"})
)
public class LinkedDataImportApplication {

  public static void main(String[] args) {
    SpringApplication.run(LinkedDataImportApplication.class, args);
  }

}
