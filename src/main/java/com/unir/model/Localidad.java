package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Localidad {
    private int id;
    private String provincia;
    private String municipio;
    private String nombre;
}
