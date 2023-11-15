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
public class EmpresasIntake {

    private static final String DATABASE = "precioCarburantes";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<Empresa> empresas = readEmpresa(connection);

            // Introducimos los datos en la base de datos
            intakeEmpresa(connection, empresas);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * Lee los datos del fichero CSV y los devuelve en una lista de empresas.
     * El fichero CSV debe estar en la raíz del proyecto.
     *
     * @return - Lista de empresas
     */
    private static List<Empresa> readEmpresa(Connection connection) {

        // Try-with-resources. Se cierra el reader automáticamente al salir del bloque try
        // CSVReader nos permite leer el fichero CSV linea a linea
        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("empresas.csv"))
                .withCSVParser(new CSVParserBuilder()
                .withSeparator(';').build()).build()) {

            // Creamos la lista de empresas y el formato de fecha
            List<Empresa> empresas = new LinkedList<>();
            // SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");

            // Saltamos la primera linea, que contiene los nombres de las columnas del CSV
            reader.skip(1);
            String[] nextLine;
            int contador = lastId(connection);

            // Leemos el fichero linea a linea
            while((nextLine = reader.readNext()) != null) {
                contador++;
                // Creamos la empresa y la añadimos a la lista
                Empresa empresa = new Empresa(
                        contador,
                        nextLine[0],
                        nextLine[1],
                        Integer.parseInt(nextLine[2])
                );
                empresas.add(empresa);
            }
            return empresas;
        } catch (IOException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Introduce los datos en la base de datos.
     * Si el empresa ya existe, se actualiza.
     * Si no existe, se inserta.
     *
     * Toma como referencia el campo nombre para determinar si el empresa existe o no.
     * @param connection - Conexión a la base de datos
     * @param empresas - Lista de empresas
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static void intakeEmpresa(Connection connection, List<Empresa> empresas) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM empresa WHERE nombre = ?";
        String insertSql = "INSERT INTO empresa (id, nombre, direccion, telefono) "
                + "VALUES (?, ?, ?, ?)";
        String updateSql = "UPDATE empresa SET nombre = ?, direccion = ?, telefono = ?";
        int lote = 5;
        int contador = 0;

        // Preparamos las consultas, una unica vez para poder reutilizarlas en el batch
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        PreparedStatement updateStatement = connection.prepareStatement(updateSql);

        // Desactivamos el autocommit para poder ejecutar el batch y hacer commit al final
        connection.setAutoCommit(false);

        for (Empresa empresa : empresas) {

            // Comprobamos si la empresa existe
            PreparedStatement selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setString(1, empresa.getNombre()); // Nombre de la empresa
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next(); // Nos movemos a la primera fila
            int rowCount = resultSet.getInt(1);

            // Si existe, actualizamos. Si no, insertamos
            if(rowCount > 0) {
                fillUpdateStatementEmpresa(updateStatement, empresa);
                updateStatement.addBatch();
            } else {
                fillInsertStatementEmpresa(insertStatement, empresa);
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
     * @param empresa - Empresa
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillInsertStatementEmpresa(PreparedStatement statement, Empresa empresa) throws SQLException {
        statement.setInt(1, empresa.getId());
        statement.setString(2, empresa.getNombre());
        statement.setString(3, empresa.getDireccion());
        statement.setInt(4, empresa.getTelefono());
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta UPDATE.
     *
     * @param statement - PreparedStatement
     * @param empresa - Empresa
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillUpdateStatementEmpresa(PreparedStatement statement, Empresa empresa) throws SQLException {
        statement.setString(1, empresa.getNombre());
        statement.setString(2, empresa.getDireccion());
        statement.setInt(3, empresa.getTelefono());
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
        String selectSql = "SELECT MAX(id) FROM empresa";
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet resultSet = selectStatement.executeQuery();
        resultSet.next(); // Nos movemos a la primera fila
        return resultSet.getInt(1);
    }
}
