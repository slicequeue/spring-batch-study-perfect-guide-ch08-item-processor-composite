package com.slicequeue.springboot.batch.domain;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * lastName 필드 값이 고유해야한다고 가정하는 경우! 레코드가 해당 사항을 준수하는지 확인하기 위해서는 lastName 추적하는 상태를 가진 유효성 검증기를 구현함
 * 검증 기능을 직접 구현하고 싶은 경우이며 유효성 검증기를
 * - org.springframework.batch.item.validator.Validator 인터페이스 구현
 * - org.springframework.batch.item.ItemStreamSupport 이용하여 각 커밋과
 */
public class UniqueLastNameValidator extends ItemStreamSupport implements Validator<Customer> {

    private Set<String> lastNames = new HashSet<>();

    @Override
    public void validate(Customer value) throws ValidationException {
        if(lastNames.contains(value.getLastName())) {
            throw new ValidationException("Duplicate last name was found: " + value.getLastName());
        }

        this.lastNames.add(value.getLastName());
    }

    // update 와 open 메서드는 Execution 간에 상태를 유지하는데 사용함

    @Override
    public void open(ExecutionContext executionContext) {
        // lastNames 필드가 이전 Execution 에 저장돼 있는지 확인함
        // 만약 저장되어 있다면 스텝 처리가 시작되기 전에 해당 갑으로 원복함
        String lastNames = getExecutionContextKey("lastNames");

        if (executionContext.containsKey(lastNames)) {
            this.lastNames = (Set<String>) executionContext.get("lastNames");
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        // update 메서드는 트랜잭션이 커밋되면 청크당 한 번 호출된다.
        // 다음 청크에 오류가 발생할 경우 현재 상태를 ExecutionContext 에 저장
        Iterator<String> itr = lastNames.iterator();
        Set<String> copiedLastNames = new HashSet<>();
        while (itr.hasNext()) {
            copiedLastNames.add(itr.next());
        }

        executionContext.put(getExecutionContextKey("lastNames"), copiedLastNames);
    }
}
