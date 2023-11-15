package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.sql.Date;

@AllArgsConstructor
@Getter
public class Gasolinera {
    private int Id;
    private Localidad Localidad;
    private int CodigoPostal;
    private String Direccion;
    private String Margen;
    private Double Longitud;
    private Double Latitud;
    private Date TomaDeDatos;
    private Precios Precios;
    private Float PorcBioalcohol;
    private Float PorcEsterMetilico;
    private Empresa Empresa;
    private String TipoVenta;
    private String Rem;
    private String Horario;
    private String TipoServicio;
}    