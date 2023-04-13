package com.akisan.universityDataMiddlePlatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class universityDataMiddlePlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(universityDataMiddlePlatformApplication.class, args);
    }
}