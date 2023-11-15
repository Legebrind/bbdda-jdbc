package com.unir.model;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public class Precios {
    private int Id;    
    private Float Gasolina95E5;
    private Float Gasolina95E10;
    private Float Gasolina95E5Premium;
    private Float Gasolina98E5;
    private Float Gasolina98E10;
    private Float GasoleoA;
    private Float GasoleoPremium;
    private Float GasoleoB;
    private Float GasoleoC;
    private Float Bioetanol;
    private Float Biodiesel;
    private Float GasesLicuadosDelPetroleo;
    private Float GasnNaturalComprimido;
    private Float GasNaturalLicuado;
    private Float Hidrogeno;
}
