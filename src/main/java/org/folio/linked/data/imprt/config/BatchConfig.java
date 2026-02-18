package org.folio.linked.data.imprt.config;

import jakarta.persistence.EntityManagerFactory;
import java.util.Set;
import javax.sql.DataSource;
import org.folio.linked.data.imprt.batch.job.processor.Rdf2LdProcessor;
import org.folio.linked.data.imprt.batch.job.reader.DatabaseRdfLineItemReader;
import org.folio.linked.data.imprt.batch.job.tasklet.FileDownloadTasklet;
import org.folio.linked.data.imprt.batch.job.tasklet.FileToDatabaseTasklet;
import org.folio.linked.data.imprt.batch.job.writer.LdKafkaSender;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

  private static final String JOB_RDF_IMPORT = "rdfImportJob";
  private static final String STEP_DOWNLOAD_FILE = "downloadFileStep";

  @Bean
  public BeanFactoryPostProcessor jobAndStepScopeConfigurer() {
    return beanFactory -> {
      var configurableBeanFactory = (ConfigurableBeanFactory) beanFactory;
      configurableBeanFactory.registerScope("job", new org.springframework.batch.core.scope.JobScope());
      configurableBeanFactory.registerScope("step", new org.springframework.batch.core.scope.StepScope());
    };
  }

  @Bean
  public JobRepository jobRepository(DataSource dataSource,
                                     PlatformTransactionManager transactionManager) throws Exception {
    var factory = new JdbcJobRepositoryFactoryBean();
    factory.setDataSource(dataSource);
    factory.setTransactionManager(transactionManager);
    factory.setTablePrefix("batch_");
    factory.afterPropertiesSet();
    return factory.getObject();
  }

  @Bean
  public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
    return new JpaTransactionManager(emf);
  }

  @Bean
  public TaskExecutor jobLauncherTaskExecutor(@Value("${mod-linked-data-import.job-pool-size}") int jobPoolSize) {
    var exec = new ThreadPoolTaskExecutor();
    exec.setMaxPoolSize(jobPoolSize);
    exec.setQueueCapacity(jobPoolSize);
    exec.setThreadNamePrefix("job-launcher-");
    exec.initialize();
    return exec;
  }

  @Bean
  public JobOperator jobOperator(JobRepository jobRepository, JobRegistry jobRegistry) throws Exception {
    var jobOperator = new TaskExecutorJobOperator();
    jobOperator.setJobRepository(jobRepository);
    jobOperator.setJobRegistry(jobRegistry);
    jobOperator.afterPropertiesSet();
    return jobOperator;
  }

  @Bean
  public MapJobRegistry jobRegistry() {
    return new MapJobRegistry();
  }

  @Bean
  public Job rdfImportJob(JobRepository jobRepository,
                          Step downloadFileStep,
                          Step fileToDatabaseStep,
                          Step mappingStep) {
    return new JobBuilder(JOB_RDF_IMPORT, jobRepository)
      .start(downloadFileStep)
      .next(fileToDatabaseStep)
      .next(mappingStep)
      .build();
  }

  @Bean
  public Step downloadFileStep(JobRepository jobRepository,
                               FileDownloadTasklet fileDownloadTasklet,
                               PlatformTransactionManager transactionManager) {
    return new StepBuilder(STEP_DOWNLOAD_FILE, jobRepository)
      .tasklet(fileDownloadTasklet, transactionManager)
      .build();
  }

  @Bean
  public Step fileToDatabaseStep(JobRepository jobRepository,
                                 FileToDatabaseTasklet fileToDatabaseTasklet,
                                 PlatformTransactionManager transactionManager) {
    return new StepBuilder("fileToDatabaseStep", jobRepository)
      .tasklet(fileToDatabaseTasklet, transactionManager)
      .build();
  }

  @Bean
  public AsyncTaskExecutor processFileTaskExecutor(
    @Value("${mod-linked-data-import.process-file.max-pool-size}") int maxPoolSize
  ) {
    var exec = new ThreadPoolTaskExecutor();
    exec.setMaxPoolSize(maxPoolSize);
    exec.setQueueCapacity(0);
    exec.setThreadNamePrefix("process-file-");
    exec.initialize();
    return exec;
  }

  @Bean
  public Step mappingStep(JobRepository jobRepository,
                          SynchronizedItemStreamReader<RdfLineWithNumber> databaseRdfLineItemReader,
                          Rdf2LdProcessor rdf2LdProcessor,
                          LdKafkaSender ldKafkaSender,
                          @Value("${mod-linked-data-import.chunk-size}") int chunkSize,
                          AsyncTaskExecutor processFileTaskExecutor) {
    return new StepBuilder("mappingStep", jobRepository)
      .<RdfLineWithNumber, Set<ResourceWithLineNumber>>chunk(chunkSize)
      .reader(databaseRdfLineItemReader)
      .processor(rdf2LdProcessor)
      .writer(ldKafkaSender)
      .taskExecutor(processFileTaskExecutor)
      .build();
  }

  @Bean
  @StepScope
  public SynchronizedItemStreamReader<RdfLineWithNumber> databaseRdfLineItemReader(
    @Value("#{stepExecution.jobExecutionId}") Long jobExecutionId,
    @Value("${mod-linked-data-import.chunk-size}") int chunkSize,
    EntityManagerFactory entityManagerFactory
  ) {
    var reader = new DatabaseRdfLineItemReader(jobExecutionId, entityManagerFactory, chunkSize);
    return new SynchronizedItemStreamReader<>(reader);
  }

}
