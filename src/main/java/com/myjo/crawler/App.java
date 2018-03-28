package com.myjo.crawler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SpringApplication app = new SpringApplication(App.class);
		app.run(args);
	}

}
