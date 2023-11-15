package com.unir.app.read;

import com.unir.config.MySqlConnector;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;

@Slf4j
public class MySqlApplication {

    private static final String DATABASE = "precioCarburantes";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos");

            selectEmpresaMasTerrestres(connection);
            selectEmpresaMasMaritimas(connection);
            selectEmpresaTerrestreMasBarata95E5Provincia(connection, "Madrid");
            selectEmpresaTerrestreMasBarataGASADistancia(connection, "Albacete", -1.855783, 38.9958, 10000);
            selectProvinciaEmpresaMaritimaMasCara95E5(connection);
        

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }    

    private static void selectEmpresaMasTerrestres(Connection connection) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement("select gasolinera.empresaId, empresa.nombre, COUNT(gasolinera.empresaId) cuenta \n" +
                "FROM gasolinera inner join\n" +
                "empresa on gasolinera.empresaId = empresa.id\n" +
                "GROUP BY empresaId\n" +
                "ORDER BY cuenta DESC LIMIT 1;\n");
        ResultSet empresa = selectEmpresa.executeQuery();

        if (empresa.next()) {
            log.debug("La empresa con mas estaciones de servicio terrestres es {} con {}",
                    empresa.getString("nombre"),
                    empresa.getString("cuenta"));
        }
    }

    private static void selectEmpresaMasMaritimas(Connection connection) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement("select embarcadero.empresaId, empresa.nombre, COUNT(embarcadero.empresaId) cuenta \n" +
                "FROM embarcadero inner join\n" +
                "empresa on embarcadero.empresaId = empresa.id\n" +
                "GROUP BY empresaId\n" +
                "ORDER BY cuenta DESC LIMIT 1;\n");
        ResultSet empresa = selectEmpresa.executeQuery();

        if (empresa.next()) {
            log.debug("La empresa con mas estaciones de servicio maritimas es {} con {}",
                    empresa.getString("nombre"),
                    empresa.getString("cuenta"));
        }
    }

    private static void selectEmpresaTerrestreMasBarata95E5Provincia(Connection connection, String provincia) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement(
            "SELECT empresa.nombre nombreEmpresa, gasolinera.direccion, gasolinera.margen, localidad.provincia, localidad.municipio, localidad.nombre nombreLocalidad, precios.gasolina95E5 \n" +
            "FROM empresa \n" +
            "INNER JOIN \n" +
                "gasolinera on  empresa.id = gasolinera.empresaId \n" +
            "INNER JOIN \n" +
                "precios on gasolinera.preciosId = precios.id \n" +
            "INNER JOIN \n" +
                "localidad on gasolinera.localidadId = localidad.id \n" +
            "where localidad.provincia = ? and gasolina95E5 > 0 \n" +
            "ORDER BY gasolina95E5 LIMIT 1");
        selectEmpresa.setString(1, provincia);

        ResultSet empresa = selectEmpresa.executeQuery();

        if (empresa.next()) {
            log.debug("La gasolinera con 95E5 mas barata en la provincia de {} está en {}, {}, {} margen {} y propiedad de {}",
                empresa.getString("provincia"),
                empresa.getString("direccion"),
                empresa.getString("nombreLocalidad"),
                empresa.getString("municipio"),
                empresa.getString("margen"),                
                empresa.getString("nombreEmpresa"));
        }
    }

    private static void selectEmpresaTerrestreMasBarataGASADistancia(Connection connection,String localidad, Double longitud, Double latitud, Integer distancia) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement(
            "SELECT ST_Distance_Sphere(POINT(latitud, longitud), POINT(?, ?)) distancia, direccion, gasoleoA \n" +
            "FROM gasolinera \n" +
            "INNER JOIN \n" +
                "precios on gasolinera.preciosId = precios.id \n" +
            "HAVING distancia <= ? \n" +
            "ORDER BY gasoleoA \n" +
            "LIMIT 1");
        selectEmpresa.setDouble(1, latitud);
        selectEmpresa.setDouble(2, longitud);
        selectEmpresa.setInt(3, distancia);

        ResultSet empresa = selectEmpresa.executeQuery();

        if (empresa.next()) {
            log.debug("A un maximo de {} km del centro de {} la gasolinera con el gasoleo A mas barato está en {} (distancia {} m). ",
                distancia/1000,
                localidad,
                empresa.getString("direccion"),
                empresa.getInt("distancia"));
        }
    }

    private static void selectProvinciaEmpresaMaritimaMasCara95E5(Connection connection) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement(
            "SELECT empresa.nombre nombreEmpresa, embarcadero.direccion, localidad.provincia, localidad.municipio, localidad.nombre nombreLocalidad, precios.gasolina95E5 \n" +
            "FROM empresa \n" +
            "INNER JOIN \n" +
                "embarcadero on  empresa.id = embarcadero.empresaId \n" +
            "INNER JOIN \n" +
                "precios on embarcadero.preciosId = precios.id \n" +
            "INNER JOIN \n" +
                "localidad on embarcadero.localidadId = localidad.id \n" +
            "where gasolina95E5 > 0 \n" +
            "ORDER BY gasolina95E5 DESC LIMIT 1");

        ResultSet empresa = selectEmpresa.executeQuery();

        if (empresa.next()) {
            log.debug("El embarcadero con 95E5 mas cara se encuentra en la provincia de {}, direccion {}, {}, {} y es propiedad de {}",
                empresa.getString("provincia"),
                empresa.getString("direccion"),
                empresa.getString("nombreLocalidad"),
                empresa.getString("municipio"),
                empresa.getString("nombreEmpresa"));
        }
    }
}
