package org.folio.linked.data.imprt.config;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;

import java.io.File;
import java.util.Set;
import javax.sql.DataSource;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.batch.job.processor.Rdf2LdProcessor;
import org.folio.linked.data.imprt.batch.job.writer.LdKafkaSender;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.scope.JobScope;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

  private static final String JOB_RDF_IMPORT = "rdfImportJob";
  private static final String STEP_DOWNLOAD_FILE = "downloadFileStep";

  @Bean
  public static BeanFactoryPostProcessor jobAndStepScopeConfigurer() {
    return beanFactory -> {
      var configurableBeanFactory = (ConfigurableBeanFactory) beanFactory;
      configurableBeanFactory.registerScope("job", new JobScope());
      configurableBeanFactory.registerScope("step", new org.springframework.batch.core.scope.StepScope());
    };
  }

  @Bean
  public JobRepository jobRepository(DataSource dataSource,
                                     PlatformTransactionManager transactionManager) throws Exception {
    var factory = new JobRepositoryFactoryBean();
    factory.setDataSource(dataSource);
    factory.setTransactionManager(transactionManager);
    factory.setTablePrefix("batch_");
    factory.afterPropertiesSet();
    return factory.getObject();
  }

  @Bean
  public PlatformTransactionManager transactionManager(DataSource dataSource) {
    return new DataSourceTransactionManager(dataSource);
  }

  @Bean
  public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
    var jobLauncher = new TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  @Bean
  public Job rdfImportJob(JobRepository jobRepository,
                          Step downloadFileStep,
                          Step processFileStep) {
    return new JobBuilder(JOB_RDF_IMPORT, jobRepository)
      .start(downloadFileStep)
      .next(processFileStep)
      .build();
  }

  @Bean
  public Step downloadFileStep(JobRepository jobRepository,
                               Tasklet fileDownloadTasklet,
                               PlatformTransactionManager transactionManager) {
    return new StepBuilder(STEP_DOWNLOAD_FILE, jobRepository)
      .tasklet(fileDownloadTasklet, transactionManager)
      .build();
  }

  @Bean
  public Step processFileStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              FlatFileItemReader<String> rdfLineItemReader,
                              Rdf2LdProcessor rdf2LdProcessor,
                              LdKafkaSender ldKafkaSender,
                              @Value("${mod-linked-data-import.chunk-size}") int chunkSize) {
    return new StepBuilder("processFileStep", jobRepository)
      .<String, Set<Resource>>chunk(chunkSize, transactionManager)
      .reader(rdfLineItemReader)
      .processor(rdf2LdProcessor)
      .writer(ldKafkaSender)
      .build();
  }

  @Bean
  @StepScope
  public FlatFileItemReader<String> rdfLineItemReader(@Value("#{jobParameters['" + FILE_URL + "']}") String fileName) {
    var file = new File(TMP_DIR, fileName);
    return new FlatFileItemReaderBuilder<String>()
      .name("lineItemReader")
      .resource(new org.springframework.core.io.FileSystemResource(file))
      .lineMapper(new PassThroughLineMapper())
      .build();
  }

}
