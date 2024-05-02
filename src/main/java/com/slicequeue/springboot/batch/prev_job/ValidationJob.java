package com.slicequeue.springboot.batch.prev_job;

import com.slicequeue.springboot.batch.domain.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

//@EnableBatchProcessing
//@SpringBootApplication
public class ValidationJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public JobParametersValidator jobParametersValidator() {
        return new DefaultJobParametersValidator(new String[]{"customerFile"}, new String[]{"run.id"});
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> customerFlatFileItemReader(@Value("#{jobParameters['customerFile']}")Resource inputFile) {
        return new FlatFileItemReaderBuilder<Customer>()
                .name("customerFlatFileItemReader")
                .delimited()
                .names("firstName",
                        "middleInitial",
                        "lastName",
                        "address",
                        "city",
                        "state",
                        "zip")
                .targetType(Customer.class)
                .resource(inputFile)
                .build();
    }

    @Bean
    public ItemWriter<Customer> itemWriter() {
        return (items) -> items.forEach(System.out::println);
    }

    // 프로세서 정의
    @Bean
    public BeanValidatingItemProcessor<Customer> customerBeanValidatingItemProcessor() {
        /*
         * ValidatingItemProcessor 의 유효성 검증 기능은 org.springframework.batch.item.validator.Validator 구현체를 통해 제공
         * 이 Validator 인터페이스는 void validate(T value) 라는 단일 메서드를 가지고 잇으며 아이템이 유효시 수행하지 않으며 다음 프로세스 또는 writer 로 진행
         * 검증 실패시 ValidationException 발행함! 따라서 BeanValidatingItemProcessor 는 JSR-303 사양에 따르는 Validator 객체를 생성하는 점에서 특별한 ItemProcessor 임
         * - 스프링 배치와 스프링 코어의 Validator 인터페이스는 동일하지 않기에 스프링 배치에서는 SpringValidator 라는 어뎁터 클래스를 제공함
         */
        return new BeanValidatingItemProcessor<>();
    }

    @Bean
    public Step copyFileStep() {
        return this.stepBuilderFactory.get("copyFileStep")
                .<Customer, Customer>chunk(5)
                .reader(customerFlatFileItemReader(null))
                .processor(customerBeanValidatingItemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return this.jobBuilderFactory.get("job-item-processor-validation")
                .validator(jobParametersValidator())
                .incrementer(new RunIdIncrementer())
                .start(copyFileStep())
                .build();
    }

    public static void main(String[] args) {
//        SpringApplication.run(ValidationJob.class, "customerFile=/input/customer-wrong.csv");
        SpringApplication.run(ValidationJob.class, "customerFile=/input/customer-success.csv");
    }
}
