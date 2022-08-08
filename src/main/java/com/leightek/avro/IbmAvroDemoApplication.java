package com.leightek.avro;

import com.leightek.avro.stream.AvroStreamDemo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IbmAvroDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(AvroStreamDemo.class, args);
	}

}
