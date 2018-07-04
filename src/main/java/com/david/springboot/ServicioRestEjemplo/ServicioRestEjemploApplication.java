package com.david.springboot.ServicioRestEjemplo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;

import com.david.springboot.ServicioRestEjemplo.swagger.SwaggerConfiguration;

@SpringBootApplication
@Import(SwaggerConfiguration.class)
public class ServicioRestEjemploApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServicioRestEjemploApplication.class, args);
	}

	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("swagger-ui.html").addResourceLocations("classpath:/META-INF/resources/");
	}
}
