package com.slicequeue.springboot.batch;

import com.slicequeue.springboot.batch.domain.Customer;
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
import org.springframework.batch.item.support.ScriptItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.util.List;

@EnableBatchProcessing
@SpringBootApplication
public class ScriptItemProcessorJob {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public JobParametersValidator jobParametersValidator() {
        return new DefaultJobParametersValidator(new String[]{"customerFile", "script"}, new String[]{"run.id"});
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

    @Bean
    @StepScope
    public ScriptItemProcessor<Customer, Customer> scriptItemProcessor(
            @Value("#{jobParameters['script']}") Resource script) {

        ScriptItemProcessor<Customer, Customer> itemProcessor =
                new ScriptItemProcessor<>();

        itemProcessor.setScript(script);

        return itemProcessor;
    }

	@Bean
	public Step copyFileStep() {

		return this.stepBuilderFactory.get("copyFileStep")
				.<Customer, Customer>chunk(5)
				.reader(customerFlatFileItemReader(null))
				.processor(scriptItemProcessor(null))
				.writer(itemWriter())
				.build();
	}

	@Bean
	public Job job() throws Exception {

		return this.jobBuilderFactory.get("job-script-item-processor-job")
                .validator(jobParametersValidator())
                .incrementer(new RunIdIncrementer())
				.start(copyFileStep())
				.build();
	}

    public static void main(String[] args) {
        System.out.println("start");
        ScriptEngineManager manager = new ScriptEngineManager();
        List<ScriptEngineFactory> engineFactories = manager.getEngineFactories();
        engineFactories.forEach(factory -> {
            System.out.println("Engine Name: " + factory.getEngineName());
            System.out.println("Engine Version: " + factory.getEngineVersion());
            System.out.println("Language: " + factory.getLanguageName());
            System.out.println("Language Version: " + factory.getLanguageVersion());
            System.out.println();
        });
        SpringApplication.run(ScriptItemProcessorJob.class,
                "customerFile=/input/customer-success.csv", "script=/upperCase.js");
        System.out.println("end");

    }

}
