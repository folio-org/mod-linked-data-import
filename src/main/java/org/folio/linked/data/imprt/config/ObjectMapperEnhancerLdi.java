package org.folio.linked.data.imprt.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.folio.ld.dictionary.model.Resource;
import org.folio.ld.dictionary.util.ResourceViewDeserializer;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
public class ObjectMapperEnhancerLdi implements BeanPostProcessor {

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(Object bean, String beanName) {
    if (bean instanceof ObjectMapper mapper) {
      SimpleModule module = new SimpleModule();
      module.addDeserializer(Resource.class, new ResourceViewDeserializer());
      mapper.registerModule(module);
    }
    return bean;
  }
}
