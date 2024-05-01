package com.slicequeue.springboot.batch;

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
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemProcessorAdapter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

//@EnableBatchProcessing
//@SpringBootApplication
public class ItemProcessAdapterJob {

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
    public ItemProcessorAdapter<Customer, Customer> itemProcessorAdapter(
            UpperCaseNameService service) { // UpperCaseNameService service 서비스 주입!
        // Adapter 생성 후 아래 설정
        ItemProcessorAdapter<Customer, Customer> adapter = new ItemProcessorAdapter<>();
        adapter.setTargetObject(service);       // 필수 - 서비스대상객체
        adapter.setTargetMethod("upperCase");   // 필수 - 메소드명
        //  adapter.setArguments(Object[] arguments); 이형태 추가로 정의 가능한데 Adapter 제네릭 부분 input Customer, output Customer 이기에
        // 별도 세팅 없이 가능 이유는 아래 주석 참고
        return adapter;
    }
    /*
     * ItemProcessorAdapter는 일반적으로 외부 서비스나 클래스의 메서드를 호출하여 데이터를 처리할 때 사용됩니다. 이 코드에서 adapter.setArguments() 없이도 작동할 수 있는 이유는 adapter.setTargetObject()와 adapter.setTargetMethod()만으로 호출할 메서드와 객체를 충분히 지정했기 때문입니다.
     *
     * 이 코드를 살펴보면, itemProcessorAdapter 메서드에서 UpperCaseNameService의 upperCase 메서드를 대상으로 설정하고 있습니다. 이때 upperCase 메서드는 Customer 객체를 받아 해당 객체의 이름을 대문자로 변환하는 작업을 수행합니다. 여기서 주어진 인자 없이도 작동하는 이유는:
     *
     * upperCase 메서드가 인자로 Customer 객체를 필요로 하며, 이는 어댑터의 제네릭 타입에 의해 이미 지정되어 있습니다.
     * 추가적인 인자를 필요로 하지 않기 때문에, setArguments를 설정할 필요가 없습니다.
     * 반면, adapter.setArguments()가 필요한 경우는 다음과 같습니다:
     *
     * 호출할 메서드가 추가적인 인자(예: 설정값, 다른 객체)를 필요로 하는 경우
     * 메서드 호출 시 전달할 인자값을 명시적으로 지정해야 하는 경우
     * 복잡한 메서드 호출 흐름이 필요하거나 다수의 인자를 전달해야 하는 경우
     * 예를 들어, UpperCaseNameService의 메서드 중 특정 기준에 따라 이름을 변경해야 할 때, 이 기준을 인자로 전달해야 한다면 setArguments를 사용해야 합니다. 이런 경우, 추가적인 정보를 전달하여 메서드의 동작을 제어할 수 있습니다.
     *
     * 결론적으로, 어댑터를 통해 호출할 메서드가 추가적인 인자를 필요로 하지 않거나, 제네릭 타입과 설정된 메서드로 충분히 작동할 수 있다면 setArguments는 필요하지 않습니다. 반면, 인자가 필요하거나 메서드의 동작에 영향을 주는 조건이 있을 때는 setArguments가 필요할 수 있습니다.
     */

    @Bean
    public ItemWriter<Customer> itemWriter() {
        return (items) -> items.forEach(System.out::println);
    }

    @Bean
    public Step copyFileStep() {
        return this.stepBuilderFactory.get("copyFileStep")
                .<Customer, Customer>chunk(5)
                .reader(customerFlatFileItemReader(null))
                .processor(itemProcessorAdapter(null))
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return this.jobBuilderFactory.get("job-item-processor-item-process-adapter")
                .validator(jobParametersValidator())
                .incrementer(new RunIdIncrementer())
                .start(copyFileStep())
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(ItemProcessAdapterJob.class, "customerFile=/input/customer-success.csv");
    }

}
