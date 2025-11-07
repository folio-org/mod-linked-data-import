package org.folio.linked.data.imprt.config;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.util.FileUtil.extractFileName;

import jakarta.persistence.EntityManagerFactory;
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
    var factory = new JobRepositoryFactoryBean();
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
    var exec = new org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor();
    exec.setMaxPoolSize(jobPoolSize);
    exec.setQueueCapacity(jobPoolSize);
    exec.setThreadNamePrefix("job-launcher-");
    exec.initialize();
    return exec;
  }

  @Bean
  public JobLauncher jobLauncher(JobRepository jobRepository,
                                 TaskExecutor jobLauncherTaskExecutor) throws Exception {
    var jobLauncher = new TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.setTaskExecutor(jobLauncherTaskExecutor);
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  @Bean
  public Job rdfImportJob(JobRepository jobRepository,
                          Step downloadFileStep,
                          Step processFileStep,
                          Step cleaningStep) {
    return new JobBuilder(JOB_RDF_IMPORT, jobRepository)
      .start(downloadFileStep)
      .next(processFileStep)
      .next(cleaningStep)
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
  public TaskExecutor processFileTaskExecutor(
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
  public Step processFileStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              FlatFileItemReader<String> rdfLineItemReader,
                              Rdf2LdProcessor rdf2LdProcessor,
                              LdKafkaSender ldKafkaSender,
                              @Value("${mod-linked-data-import.chunk-size}") int chunkSize,
                              TaskExecutor processFileTaskExecutor) {
    return new StepBuilder("processFileStep", jobRepository)
      .<String, Set<Resource>>chunk(chunkSize, transactionManager)
      .reader(rdfLineItemReader)
      .processor(rdf2LdProcessor)
      .writer(ldKafkaSender)
      .taskExecutor(processFileTaskExecutor)
      .build();
  }

  @Bean
  @StepScope
  public FlatFileItemReader<String> rdfLineItemReader(@Value("#{jobParameters['" + FILE_URL + "']}") String fileUrl) {
    var file = new File(TMP_DIR, extractFileName(fileUrl));
    return new FlatFileItemReaderBuilder<String>()
      .name("lineItemReader")
      .resource(new org.springframework.core.io.FileSystemResource(file))
      .lineMapper(new PassThroughLineMapper())
      .build();
  }

  @Bean
  public Step cleaningStep(JobRepository jobRepository,
                           Tasklet fileCleanupTasklet,
                           PlatformTransactionManager transactionManager) {
    return new StepBuilder("cleaningStep", jobRepository)
      .tasklet(fileCleanupTasklet, transactionManager)
      .build();
  }

}
