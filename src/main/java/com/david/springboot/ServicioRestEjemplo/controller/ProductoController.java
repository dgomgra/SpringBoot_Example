package com.david.springboot.ServicioRestEjemplo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.david.springboot.ServicioRestEjemplo.model.Producto;
import com.david.springboot.ServicioRestEjemplo.service.ProductoService;

@RestController
@RequestMapping("/productos")
public class ProductoController {

	@Autowired
	ProductoService productoService;
	
	@RequestMapping(value = "/{id}", method = RequestMethod.GET)
	public Producto getProducto(@PathVariable Integer id) {
		Producto prod = productoService.findById(id);
		
		return prod;
	}
}
