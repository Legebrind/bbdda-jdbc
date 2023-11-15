package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Empresa {
    private int Id;
    private String Nombre;
    private String Direccion;
    private int Telefono;
}
