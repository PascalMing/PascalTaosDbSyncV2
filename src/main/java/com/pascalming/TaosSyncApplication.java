package com.pascalming;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.pascalming.*"})
@EnableScheduling
public class TaosSyncApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaosSyncApplication.class);
    }
}