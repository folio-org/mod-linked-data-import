package org.folio.linked.data.imprt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.resilience.annotation.EnableResilientMethods;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableAsync
@EnableCaching
@EnableScheduling
@EnableResilientMethods
@SpringBootApplication
@ComponentScan(value = "org.folio")
public class LinkedDataImportApplication {

  public static void main(String[] args) {
    SpringApplication.run(LinkedDataImportApplication.class, args);
  }

}
