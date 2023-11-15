package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Embarcadero {
    private int Id;
    private Localidad Localidad;
    private int CodigoPostal;
    private String Direccion;
    private Double Longitud;
    private Double Latitud;
    private Precios Precios;
    private Float GasoleoUsoMaritimo;
    private Empresa Empresa;
    private String TipoVenta;
    private String Rem;
    private String Horario;
}    