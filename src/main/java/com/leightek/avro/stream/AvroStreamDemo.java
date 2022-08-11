package com.leightek.avro.stream;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.leightek.avro.consumer.BalanceAvroMessageConsumer;
import com.leightek.avro.consumer.CustomerAvroMessageConsumer;
import com.leightek.avro.producer.BalanceAvroMessageProducer;
import com.leightek.avro.producer.CustomerAvroMessageProducer;
import com.leightek.avro.producer.CustomerBalanceAvroMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AvroStreamDemo implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(AvroStreamDemo.class);

    private CustomerAvroMessageProducer customerProducer;
    private BalanceAvroMessageProducer balanceProducer;
    private CustomerBalanceAvroMessageProducer customerBalanceProducer;
    private CustomerAvroMessageConsumer customerConsumer;
    private BalanceAvroMessageConsumer balanceConsumer;

    public void setCustomerProducer(CustomerAvroMessageProducer customerProducer) {
        this.customerProducer = customerProducer;
    }

    public void setBalanceProducer(BalanceAvroMessageProducer balanceProducer) {
        this.balanceProducer = balanceProducer;
    }

    public void setCustomerBalanceProducer(CustomerBalanceAvroMessageProducer customerBalanceProducer) {
        this.customerBalanceProducer = customerBalanceProducer;
    }

    public void setCustomerConsumer(CustomerAvroMessageConsumer customerConsumer) {
        this.customerConsumer = customerConsumer;
    }

    public void setBalanceConsumer(BalanceAvroMessageConsumer balanceConsumer) {
        this.balanceConsumer = balanceConsumer;
    }

    @Override
    public void run() {

        logger.info("producing messages ...");
        customerProducer.produceMessage();
        balanceProducer.produceMessage();

        List<Customer> customers = new ArrayList<>();
        List<Object> newCustomers;
        List<Balance> balanceList = new ArrayList<>();
        List<Object> newBalanceList;

        List<CustomerBalance> customerBalanceList = new ArrayList<>();
        String customerBalanceTopic = "CustomerBalance";
        Set<String> accountIds = new HashSet<>();

        while (true) {
            logger.info("consuming messages ...");

            newCustomers = customerConsumer.consumeMessage();
            if (!CollectionUtils.isEmpty(newCustomers)) {
                addToCustomerList(customers, newCustomers);
            }

            newBalanceList = balanceConsumer.consumeMessage();

            if (!CollectionUtils.isEmpty(newBalanceList)) {
                addToBalanceList(balanceList, newBalanceList);
            }

            if (!customers.isEmpty() && !balanceList.isEmpty()) {
                for (Customer customer : customers) {
                    mergeCustomerBalance(customer, balanceList,customerBalanceList, accountIds);
                }

                if (!customerBalanceList.isEmpty()) {
                    customerBalanceList.stream().forEach(customerBalance ->
                            customerBalanceProducer.produceMessage(customerBalanceTopic, customerBalance));

                    customers = customers.stream()
                            .filter(customer -> !accountIds.contains(customer.getAccountId()))
                            .collect(Collectors.toList());
                    balanceList = balanceList.stream()
                            .filter(balance -> !accountIds.contains(balance.getAccountId()))
                            .collect(Collectors.toList());
                    accountIds.clear();
                }
            }
        }

    }

    private void addToCustomerList(List<Customer> customers, List<Object> newCustomers) {
        for (Object customer : newCustomers) {
            if (customer != null) {
                customers.add((Customer) customer);
            }
        }
    }

    private void addToBalanceList(List<Balance> balanceList, List<Object> newBalanceList) {
        for (Object balance : newBalanceList) {
            if (balance != null) {
                balanceList.add((Balance) balance);
            }
        }
    }

    private static void mergeCustomerBalance(Customer customer, List<Balance> balanceList,
                                             List<CustomerBalance> customerBalanceList, Set<String> accountIds) {

        logger.info("merging messages ...");

        balanceList.forEach(balance -> {
            if (Objects.equals(customer.getAccountId(), balance.getAccountId())) {
                CustomerBalance customerBalance = CustomerBalance.newBuilder()
                        .setAccountId(customer.getAccountId())
                        .setCustomerId(customer.getCustomerId())
                        .setPhoneNumber(customer.getPhoneNumber())
                        .setBalance(balance.getBalance())
                        .build();
                customerBalanceList.add(customerBalance);
                accountIds.add(customer.getAccountId());
            }
        });
    }
}
