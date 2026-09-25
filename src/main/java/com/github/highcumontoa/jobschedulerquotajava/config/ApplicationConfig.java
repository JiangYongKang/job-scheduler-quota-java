package com.github.highcumontoa.jobschedulerquotajava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Bean 装配：统一时钟（测试可替换为可控时钟）、调度参数。
 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class ApplicationConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
