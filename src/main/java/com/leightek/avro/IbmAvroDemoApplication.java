package com.leightek.avro;

import com.leightek.avro.stream.AvroStreamDemo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class IbmAvroDemoApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext context = SpringApplication.run(AvroStreamDemo.class, args);

		AvroStreamDemo avroStreamDemo = context.getBean(AvroStreamDemo.class);
		avroStreamDemo.run();

		context.close();
	}

}
