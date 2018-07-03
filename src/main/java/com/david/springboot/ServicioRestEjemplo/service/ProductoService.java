package com.david.springboot.ServicioRestEjemplo.service;

import com.david.springboot.ServicioRestEjemplo.model.Producto;

public interface ProductoService {
	Producto findById(Integer id);
}
