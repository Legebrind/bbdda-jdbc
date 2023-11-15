package com.unir.app.write;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.unir.config.MySqlConnector;
import com.unir.model.*;
import lombok.extern.slf4j.Slf4j;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * La version para Oracle seria muy similar a esta, cambiando únicamente el Driver y los datos de sentencias.
 * La tabla de Oracle contiene muchas restricciones y triggers. Por simplicidad, usamos MySQL en este caso.
 */
@Slf4j
public class LocalidadesIntake {

    private static final String DATABASE = "precioCarburantes";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<Localidad> localidades = readLocalidad(connection);

            // Introducimos los datos en la base de datos
            intakeLocalidad(connection, localidades);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * Lee los datos del fichero CSV y los devuelve en una lista de localidades.
     * El fichero CSV debe estar en la raíz del proyecto.
     *
     * @return - Lista de localidades
     */
    private static List<Localidad> readLocalidad(Connection connection) {
        

        // Try-with-resources. Se cierra el reader automáticamente al salir del bloque try
        // CSVReader nos permite leer el fichero CSV linea a linea
        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("Localidades.csv"))
                .withCSVParser(new CSVParserBuilder()
                .withSeparator(';').build()).build()) {

            // Creamos la lista de localidades y el formato de fecha
            List<Localidad> localidades = new LinkedList<>();
            // SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");

            // Saltamos la primera linea, que contiene los nombres de las columnas del CSV
            reader.skip(1);
            String[] nextLine;
            int contador = lastId(connection);

            // Leemos el fichero linea a linea
            while((nextLine = reader.readNext()) != null) {
                contador++;
                // Creamos la localidad y la añadimos a la lista
                Localidad localidad = new Localidad(
                        contador,
                        nextLine[0],
                        nextLine[1],
                        nextLine[2]
                );
                localidades.add(localidad);
            }
            return localidades;
        } catch (IOException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Introduce los datos en la base de datos.
     * Si la localidad ya existe, se actualiza.
     * Si no existe, se inserta.
     *
     * Toma como referencia el campo emp_no para determinar si la localidad existe o no.
     * @param connection - Conexión a la base de datos
     * @param localidades - Lista de localidades
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static void intakeLocalidad(Connection connection, List<Localidad> localidades) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM localidad WHERE nombre = ?";
        String insertSql = "INSERT INTO localidad (id, provincia, municipio, nombre) "
                + "VALUES (?, ?, ?, ?)";
        String updateSql = "UPDATE localidad SET provincia = ?, municipio = ?, nombre = ?";
        int lote = 5;
        int contador = 0;

        // Preparamos las consultas, una unica vez para poder reutilizarlas en el batch
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        PreparedStatement updateStatement = connection.prepareStatement(updateSql);

        // Desactivamos el autocommit para poder ejecutar el batch y hacer commit al final
        connection.setAutoCommit(false);

        for (Localidad localidad : localidades) {

            // Comprobamos si la localidad existe
            PreparedStatement selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setString(1, localidad.getNombre()); // Nombre de la localidad
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next(); // Nos movemos a la primera fila
            int rowCount = resultSet.getInt(1);

            // Si existe, actualizamos. Si no, insertamos
            if(rowCount > 0) {
                fillUpdateStatementLocalidad(updateStatement, localidad);
                updateStatement.addBatch();
            } else {
                fillInsertStatementLocalidad(insertStatement, localidad);
                insertStatement.addBatch();
            }

            // Ejecutamos el batch cada lote de registros
            if (++contador % lote == 0) {
                updateStatement.executeBatch();
                insertStatement.executeBatch();
            }
        }

        // Ejecutamos el batch final
        insertStatement.executeBatch();
        updateStatement.executeBatch();

        // Hacemos commit y volvemos a activar el autocommit
        connection.commit();
        connection.setAutoCommit(true);
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta INSERT.
     *
     * @param statement - PreparedStatement
     * @param localidad - Localidad
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillInsertStatementLocalidad(PreparedStatement statement, Localidad localidad) throws SQLException {
        statement.setInt(1, localidad.getId());
        statement.setString(2, localidad.getProvincia());
        statement.setString(3, localidad.getMunicipio());
        statement.setString(4, localidad.getNombre());
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta UPDATE.
     *
     * @param statement - PreparedStatement
     * @param localidad - Localidad
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillUpdateStatementLocalidad(PreparedStatement statement, Localidad localidad) throws SQLException {
        statement.setString(1, localidad.getProvincia());
        statement.setString(2, localidad.getMunicipio());
        statement.setString(3, localidad.getNombre());
    }

    /**
     * Devuelve el último id de una columna de una tabla.
     * Util para obtener el siguiente id a insertar.
     *
     * @param connection - Conexión a la base de datos
     * @param table - Nombre de la tabla
     * @param fieldName - Nombre de la columna
     * @return - Último id de la columna
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static int lastId(Connection connection) throws SQLException {
        String selectSql = "SELECT MAX(id) FROM localidad";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet resultSet = selectStatement.executeQuery();
        resultSet.next(); // Nos movemos a la primera fila
        return resultSet.getInt(1);
    }
}
