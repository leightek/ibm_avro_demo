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

import java.util.*;
import java.util.stream.Collectors;

public class AvroStreamDemo implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(AvroStreamDemo.class);

    private CustomerAvroMessageProducer customerProducer;
    private BalanceAvroMessageProducer balanceProducer;
    private CustomerBalanceAvroMessageProducer customerBalanceProducer;

    private CustomerAvroMessageConsumer customerConsumer;
    private BalanceAvroMessageConsumer balanceConsumer;

    public AvroStreamDemo(CustomerAvroMessageProducer customerProducer,
                          BalanceAvroMessageProducer balanceProducer,
                          CustomerBalanceAvroMessageProducer customerBalanceProducer,
                          CustomerAvroMessageConsumer customerConsumer,
                          BalanceAvroMessageConsumer balanceConsumer) {
        this.customerProducer = customerProducer;
        this.balanceProducer = balanceProducer;
        this.customerBalanceProducer = customerBalanceProducer;
        this.customerConsumer = customerConsumer;
        this.balanceConsumer = balanceConsumer;
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

    @Override
    public void run() {

        logger.info("producing messages ...");
        customerProducer.produceMessage();
        balanceProducer.produceMessage();

        List<Customer> customers = new ArrayList<>();
        List<Balance> balanceList = new ArrayList<>();
        List<CustomerBalance> customerBalanceList = new ArrayList<>();
        Set<String> accountIds = new HashSet<>();

        while (true) {
            logger.info("coonsuming messages ...");
            customers = customerConsumer.consumeMessage(customers);
            balanceList = balanceConsumer.consumeMessage(balanceList);

            if (!customers.isEmpty() && !balanceList.isEmpty()) {
                for (Customer customer : customers) {
                    mergeCustomerBalance(customer, balanceList,customerBalanceList, accountIds);
                }
                if (!customerBalanceList.isEmpty()) {
                    customerBalanceList.stream().forEach(customerBalance ->
                            customerBalanceProducer.produceMessage(customerBalance));

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
}
