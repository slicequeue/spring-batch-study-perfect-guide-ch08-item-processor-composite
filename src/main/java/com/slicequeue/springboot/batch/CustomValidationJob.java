package com.slicequeue.springboot.batch;

import com.slicequeue.springboot.batch.domain.Customer;
import com.slicequeue.springboot.batch.domain.UniqueLastNameValidator;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

//@EnableBatchProcessing
//@SpringBootApplication
public class CustomValidationJob {

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
    public FlatFileItemReader<Customer> customerFlatFileItemReader(@Value("#{jobParameters['customerFile']}") Resource inputFile) {
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
    public ValidatingItemProcessor<Customer> customerValidatingItemProcessor() {
        return new ValidatingItemProcessor<>(validator()); // UniqueLastNameValidator 관련 빈 주입하여 ValidatingItemProcessor 빈 생성
    }

    @Bean
    public UniqueLastNameValidator validator() { // UniqueLastNameValidator 빈 등록
        UniqueLastNameValidator uniqueLastNameValidator = new UniqueLastNameValidator();
        uniqueLastNameValidator.setName("validator");
        return uniqueLastNameValidator;
    }

    @Bean
    public Step copyFileStep() {
        return this.stepBuilderFactory.get("copyFileStep")
                .<Customer, Customer>chunk(5)
                .reader(customerFlatFileItemReader(null))
                .processor(customerValidatingItemProcessor())
                .writer(itemWriter())
                .stream(validator()) // ItemStream 관련 메서드를 호출할 수 잇도록 등록 -> UniqueLastNameValidator 에서 ItemStreamSupport 상속 구현 한 것 적용되도록!
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return this.jobBuilderFactory.get("job-item-processor-custom-validation")
                .validator(jobParametersValidator())
                .incrementer(new RunIdIncrementer())
                .start(copyFileStep())
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(CustomValidationJob.class, "customerFile=/input/customer-unique.csv");
        // 1차 시도: org.springframework.batch.item.validator.ValidationException: Duplicate last name was found: Darrow
        // 6번째 줄 삭제
        // 2차 시도: org.springframework.batch.item.validator.ValidationException: Duplicate last name was found: Darrow
        // 12번쨰 줄 삭제
        //
    }
}
