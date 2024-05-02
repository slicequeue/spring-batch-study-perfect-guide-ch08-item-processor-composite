package com.slicequeue.springboot.batch;


import com.slicequeue.springboot.batch.batch.ZipCodeClassifier;
import com.slicequeue.springboot.batch.domain.Customer;
import com.slicequeue.springboot.batch.service.UpperCaseNameService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemProcessor;
import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

@EnableBatchProcessing
@SpringBootApplication
public class ClassifierCompositeItemProcessorJob {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public JobParametersValidator jobParametersValidator() {
        return new DefaultJobParametersValidator(new String[]{"customerFile", "script"}, new String[]{"run.id"});
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> customerFlatFileItemReader(
            @Value("#{jobParameters['customerFile']}") Resource inputFile) {

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

    @Bean // ItemProcessor 2 - ItemProcessorAdapter 활용한 고객이름을 대문자로 변경
    public ItemProcessorAdapter<Customer, Customer> upperCaseItemProcessor(UpperCaseNameService service) {
        ItemProcessorAdapter<Customer, Customer> adapter = new ItemProcessorAdapter<>();

        adapter.setTargetObject(service);
        adapter.setTargetMethod("upperCase");

        return adapter;
    }

    @Bean // ItemProcessor 3 - ScriptItemProcessor 활용한 고객의 모든 주소 관련 필드를 소문자로 변경
    @StepScope
    public ScriptItemProcessor<Customer, Customer> lowerCaseItemProcessor(@Value("#{jobParameters['script']}") Resource script) {
        ScriptItemProcessor<Customer, Customer> itemProcessor = new ScriptItemProcessor<>();

        itemProcessor.setScript(script);

        return itemProcessor;
    }

    @Bean
    public Classifier classifier() {
        return new ZipCodeClassifier(
                upperCaseItemProcessor(null),      // oddItemProcessor
                lowerCaseItemProcessor(null)        // evenItemProcessor
        );
    }

    @Bean
    public ClassifierCompositeItemProcessor<Customer, Customer> itemProcessor() {
        ClassifierCompositeItemProcessor<Customer, Customer> itemProcessor =
                new ClassifierCompositeItemProcessor<>(); // ClassifierCompositeItemProcessor 통한 프로세스 주입

        itemProcessor.setClassifier(classifier()); // 분류기 설정

        return itemProcessor;
    }

    @Bean
    public ItemWriter<Customer> itemWriter() {
        return (items) -> items.forEach(System.out::println);
    }

    @Bean
    public Step copyFileStep() {

        return this.stepBuilderFactory.get("step-item-processor-classifier-composite")
                .<Customer, Customer>chunk(5)
                .reader(customerFlatFileItemReader(null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {

        return this.jobBuilderFactory.get("job-item-processor-classifier-composite")
                .validator(jobParametersValidator())
                .start(copyFileStep())
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(ClassifierCompositeItemProcessorJob.class, "customerFile=/input/customer-unique.csv", "script=/lowerCase.js");
    }

}
