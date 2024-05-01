package com.slicequeue.springboot.batch.domain;

import com.slicequeue.springboot.batch.common.Constants;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import static com.slicequeue.springboot.batch.common.Constants.Messages.VALIDATION_FAIL_POSTFIX_MUST_BE_ONLY_ALPHABET;
import static com.slicequeue.springboot.batch.common.Constants.Messages.VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL;
import static com.slicequeue.springboot.batch.common.Constants.REGEX_ONLY_ALPHABET;

public class Customer {

    @NotNull(message = "First name" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Pattern(regexp = REGEX_ONLY_ALPHABET, message = "First name" + VALIDATION_FAIL_POSTFIX_MUST_BE_ONLY_ALPHABET)
    private String firstName;

    @Size(min = 1, max = 1)
    @Pattern(regexp = REGEX_ONLY_ALPHABET, message = "Middle initial" + VALIDATION_FAIL_POSTFIX_MUST_BE_ONLY_ALPHABET)
    private String middleInitial;

    @NotNull(message = "Last name" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Pattern(regexp = REGEX_ONLY_ALPHABET, message = "Last name" + VALIDATION_FAIL_POSTFIX_MUST_BE_ONLY_ALPHABET)
    private String lastName;

    @NotNull(message = "Address" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Pattern(regexp = "[0-9a-zA-Z\\. ]+")
    private String address;

    @NotNull(message = "Address" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Pattern(regexp = "[a-zA-Z\\. ]+")
    private String city;

    @NotNull(message = "state" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Size(min = 2, max = 2)
    @Pattern(regexp = "[A-Z]{2}")
    private String state;

    @NotNull(message = "Zip" + VALIDATION_FAIL_POSTFIX_MUST_NOT_BE_NULL)
    @Size(min = 5, max = 5)
    @Pattern(regexp = "\\d{5}")
    private String zip;

    public Customer() {
    }

    public Customer(String firstName, String middleInitial, String lastName, String address, String city, String state, String zip) {
        this.firstName = firstName;
        this.middleInitial = middleInitial;
        this.lastName = lastName;
        this.address = address;
        this.city = city;
        this.state = state;
        this.zip = zip;
    }

    public Customer(Customer original) {
        this.firstName = original.getFirstName();
        this.middleInitial = original.getMiddleInitial();
        this.lastName = original.getLastName();
        this.address = original.getAddress();
        this.city = original.getCity();
        this.state = original.getState();
        this.zip = original.getZip();
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getMiddleInitial() {
        return middleInitial;
    }

    public void setMiddleInitial(String middleInitial) {
        this.middleInitial = middleInitial;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "firstName='" + firstName + '\'' +
                ", middleInitial='" + middleInitial + '\'' +
                ", lastName='" + lastName + '\'' +
                ", address='" + address + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", zip='" + zip + '\'' +
                '}';
    }
}
