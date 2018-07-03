package com.david.springboot.ServicioRestEjemplo.service;

import org.springframework.stereotype.Service;

import com.david.springboot.ServicioRestEjemplo.model.Producto;

@Service
public class ProductoServiceImpl  implements ProductoService{

	@Override
	public Producto findById(Integer id) {
		Producto prod = new Producto(1, "datos", 1234d);
		return prod;
	}

}
