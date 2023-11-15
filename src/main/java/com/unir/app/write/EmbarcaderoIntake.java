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
public class EmbarcaderoIntake {

    private static final String DATABASE = "precioCarburantes";

    public static void main(String[] args) {

        //Creamos conexion. No es necesario indicar puerto en host si usamos el default, 1521
        //Try-with-resources. Se cierra la conexión automáticamente al salir del bloque try
        try(Connection connection = new MySqlConnector("localhost", DATABASE).getConnection()) {

            log.info("Conexión establecida con la base de datos MySQL");

            // Leemos los datos del fichero CSV
            List<Embarcadero> embarcaderos = readEmbarcadero(connection);

            // Introducimos los datos en la base de datos
            
            intakeEmbarcadero(connection, embarcaderos);

        } catch (Exception e) {
            log.error("Error al tratar con la base de datos", e);
        }
    }

    /**
     * Lee los datos del fichero CSV y los devuelve en una lista de embarcaderos.
     * El fichero CSV debe estar en la raíz del proyecto.
     *
     * @return - Lista de embarcaderos
     */
    private static List<Embarcadero> readEmbarcadero(Connection connection) {

        // Try-with-resources. Se cierra el reader automáticamente al salir del bloque try
        // CSVReader nos permite leer el fichero CSV linea a linea
        try (CSVReader reader = new CSVReaderBuilder(
                new FileReader("embarcacionesPrecios_es.csv"))
                .withCSVParser(new CSVParserBuilder()
                .withSeparator(';').build()).build()) {

            // Creamos la lista de embarcaderos y el formato de fecha
            List<Embarcadero> embarcaderos = new LinkedList<>();
            List<Precios> precios = new LinkedList<>();

            // Saltamos la primera linea, que contiene los nombres de las columnas del CSV
            reader.skip(1);
            String[] nextLine;
            int contadorEmbarcadero = lastId(connection, false);
            int contadorPrecios = lastId(connection, true);

            // Leemos el fichero linea a linea
            while((nextLine = reader.readNext()) != null) {
                contadorPrecios++;
                // Buscamos la localidad
                Localidad localidadDeEmbarcadero = selectLocalidad(connection, nextLine[0]);
                // Buscamos la empresa
                Empresa empresaDeEmbarcadero = selectEmpresa(connection, nextLine[10]);
                // Creamos los precios y los añadimos a la lista
                Precios preciosDeEmbarcadero = new Precios(
                    contadorPrecios,
                    parseFloat(nextLine[5]),
                    parseFloat(nextLine[6]),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(nextLine[7]),
                    parseFloat(null),
                    parseFloat(nextLine[8]),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null),
                    parseFloat(null));

                precios.add(preciosDeEmbarcadero);
                // Creamos la embarcadero y la añadimos a la lista
                contadorEmbarcadero++;
                Embarcadero embarcadero = new Embarcadero(
                    contadorEmbarcadero,
                    localidadDeEmbarcadero,
                    Integer.parseInt(nextLine[1]),
                    nextLine[2],
                    Double.parseDouble(nextLine[3].replace(",", ".")),
                    Double.parseDouble(nextLine[4].replace(",", ".")),
                    preciosDeEmbarcadero,
                    parseFloat(nextLine[9]),
                    empresaDeEmbarcadero,
                    nextLine[11],
                    nextLine[12],
                    nextLine[13]);
            
                embarcaderos.add(embarcadero);
            }
            intakePrecios(connection, precios);
            return embarcaderos;
        } catch (IOException e) {
            log.error("Error al leer el fichero CSV", e);
            throw new RuntimeException(e);
        } catch (CsvValidationException | SQLException e) {
            throw new RuntimeException(e);
        }
        
    }

    /**
     * Introduce los datos en la base de datos.
     * Si el embarcadero ya existe, se actualiza.
     * Si no existe, se inserta.
     *
     * Toma como referencia el campo emp_no para determinar si el embarcadero existe o no.
     * @param connection - Conexión a la base de datos
     * @param embarcaderos - Lista de embarcaderos
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static void intakeEmbarcadero(Connection connection, List<Embarcadero> embarcaderos) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM embarcadero WHERE longitud = ? and latitud = ?";
        String insertSql = "INSERT INTO embarcadero (id, localidadId, codigoPostal, direccion, longitud, latitud, preciosId, gasoleoUsoMaritimo, empresaId, tipoVenta, rem, horario) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE embarcadero SET localidadId = ?, codigoPostal = ?, direccion = ?, longitud = ?, latitud = ?, preciosId = ?, gasoleoUsoMaritimo = ?, empresaId = ?, tipoVenta = ?, rem = ?, horario = ?";
        int lote = 5;
        int contador = 0;

        // Preparamos las consultas, una unica vez para poder reutilizarlas en el batch
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        PreparedStatement updateStatement = connection.prepareStatement(updateSql);

        // Desactivamos el autocommit para poder ejecutar el batch y hacer commit al final
        connection.setAutoCommit(false);

        for (Embarcadero embarcadero : embarcaderos) {

            // Comprobamos si la embarcadero existe
            PreparedStatement selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setDouble(1, embarcadero.getLongitud());
            selectStatement.setDouble(2, embarcadero.getLatitud());
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next(); // Nos movemos a la primera fila
            int rowCount = resultSet.getInt(1);

            // Si existe, actualizamos. Si no, insertamos
            if(rowCount > 0) {
                fillUpdateStatementEmbarcadero(updateStatement, embarcadero);
                updateStatement.addBatch();
            } else {
                fillInsertStatementEmbarcadero(insertStatement, embarcadero);
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
     * @param embarcadero - Embarcadero
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillInsertStatementEmbarcadero(PreparedStatement statement, Embarcadero embarcadero) throws SQLException {
        
        if(embarcadero.getEmpresa() == null){
             statement.setInt(9, 0);
        }else{
            statement.setInt(9, embarcadero.getEmpresa().getId());
        }
        statement.setInt(1, embarcadero.getId());
        statement.setInt(2, embarcadero.getLocalidad().getId());
        statement.setInt(3, embarcadero.getCodigoPostal());
        statement.setString(4, embarcadero.getDireccion());
        statement.setDouble(5, embarcadero.getLongitud());
        statement.setDouble(6, embarcadero.getLatitud());
        statement.setInt(7, embarcadero.getPrecios().getId());
        statement.setFloat(8, embarcadero.getGasoleoUsoMaritimo());
        statement.setString(10, embarcadero.getTipoVenta());
        statement.setString(11, embarcadero.getRem());
        statement.setString(12, embarcadero.getHorario());
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta UPDATE.
     *
     * @param statement - PreparedStatement
     * @param embarcadero - Embarcadero
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillUpdateStatementEmbarcadero(PreparedStatement statement, Embarcadero embarcadero) throws SQLException {

        if(embarcadero.getEmpresa() == null){
             statement.setInt(11, 0);
        }else{
            statement.setInt(11, embarcadero.getEmpresa().getId());
        }
        statement.setInt(1, embarcadero.getLocalidad().getId());
        statement.setInt(2, embarcadero.getCodigoPostal());
        statement.setString(3, embarcadero.getDireccion());
        statement.setDouble(5, embarcadero.getLongitud());
        statement.setDouble(6, embarcadero.getLatitud());
        statement.setInt(8, embarcadero.getPrecios().getId());
        statement.setFloat(10, embarcadero.getGasoleoUsoMaritimo());
        statement.setString(12, embarcadero.getTipoVenta());
        statement.setString(13, embarcadero.getRem());
        statement.setString(14, embarcadero.getHorario());
    }

    /**
     * Introduce los datos en la base de datos.
     * Si el precio ya existe, se actualiza.
     * Si no existe, se inserta.
     *
     * Toma como referencia el campo id para determinar si los precios existen o no.
     * @param connection - Conexión a la base de datos
     * @param precios - Lista de precios
     * @throws SQLException - Error al ejecutar la consulta
     */
    private static void intakePrecios(Connection connection, List<Precios> precios) throws SQLException {

        String selectSql = "SELECT COUNT(*) FROM precios WHERE id = ?";
        String insertSql = "INSERT INTO precios (id, gasolina95E5, gasolina95E10, gasolina95E5Premium, gasolina98E5, gasolina98E10, gasoleoA, gasoleoPremium, gasoleoB, gasoleoC, bioetanol, biodiesel, gasesLicuadosDelPetroleo, gasnNaturalComprimido, gasNaturalLicuado, hidrogeno) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String updateSql = "UPDATE precios SET gasolina95E5 = ?, gasolina95E10 = ?, gasolina95E5Premium = ?, gasolina98E5 = ?, gasolina98E10 = ?, gasoleoA = ?, gasoleoPremium = ?, gasoleoB = ?, gasoleoC = ?, bioetanol = ?, biodiesel = ?, gasesLicuadosDelPetroleo = ?, gasnNaturalComprimido = ?, gasNaturalLicuado = ?, hidrogeno = ?";
        int lote = 5;
        int contador = 0;

        // Preparamos las consultas, una unica vez para poder reutilizarlas en el batch
        PreparedStatement insertStatement = connection.prepareStatement(insertSql);
        PreparedStatement updateStatement = connection.prepareStatement(updateSql);

        // Desactivamos el autocommit para poder ejecutar el batch y hacer commit al final
        connection.setAutoCommit(false);

        for (Precios preciosDeEmbarcadero : precios) {

            // Comprobamos si los precios existen
            PreparedStatement selectStatement = connection.prepareStatement(selectSql);
            selectStatement.setInt(1, preciosDeEmbarcadero.getId()); // Código de la embarcadero
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next(); // Nos movemos a la primera fila
            int rowCount = resultSet.getInt(1);

            // Si existe, actualizamos. Si no, insertamos
            if(rowCount > 0) {
                fillUpdateStatementPrecios(updateStatement, preciosDeEmbarcadero);
                updateStatement.addBatch();
            } else {
                fillInsertStatementPrecios(insertStatement, preciosDeEmbarcadero);
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
     * @param embarcadero - Embarcadero
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillInsertStatementPrecios(PreparedStatement statement, Precios precios) throws SQLException {
        statement.setInt(1, precios.getId());
        statement.setFloat(2, precios.getGasolina95E5());
        statement.setFloat(3, precios.getGasolina95E10());
        statement.setFloat(4, precios.getGasolina95E5Premium());
        statement.setFloat(5, precios.getGasolina98E5());
        statement.setFloat(6, precios.getGasolina98E10());
        statement.setFloat(7, precios.getGasoleoA());
        statement.setFloat(8, precios.getGasoleoPremium());
        statement.setFloat(9, precios.getGasoleoB());
        statement.setFloat(10, precios.getGasoleoC());
        statement.setFloat(11, precios.getBioetanol());
        statement.setFloat(12, precios.getBiodiesel());
        statement.setFloat(13, precios.getGasesLicuadosDelPetroleo());
        statement.setFloat(14, precios.getGasnNaturalComprimido());
        statement.setFloat(15, precios.getGasNaturalLicuado());
        statement.setFloat(16, precios.getHidrogeno());
    }

    /**
     * Rellena los parámetros de un PreparedStatement para una consulta UPDATE.
     *
     * @param statement - PreparedStatement
     * @param embarcadero - Embarcadero
     * @throws SQLException - Error al rellenar los parámetros
     */
    private static void fillUpdateStatementPrecios(PreparedStatement statement, Precios precios) throws SQLException {
        statement.setFloat(1, precios.getGasolina95E5());
        statement.setFloat(2, precios.getGasolina95E10());
        statement.setFloat(3, precios.getGasolina95E5Premium());
        statement.setFloat(4, precios.getGasolina98E5());
        statement.setFloat(5, precios.getGasolina98E10());
        statement.setFloat(6, precios.getGasoleoA());
        statement.setFloat(7, precios.getGasoleoPremium());
        statement.setFloat(8, precios.getGasoleoB());
        statement.setFloat(9, precios.getGasoleoC());
        statement.setFloat(10, precios.getBioetanol());
        statement.setFloat(11, precios.getBiodiesel());
        statement.setFloat(12, precios.getGasesLicuadosDelPetroleo());
        statement.setFloat(13, precios.getGasnNaturalComprimido());
        statement.setFloat(14, precios.getGasNaturalLicuado());
        statement.setFloat(15, precios.getHidrogeno());
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
    private static int lastId(Connection connection, Boolean isPrecio) throws SQLException {
        String selectSql = "SELECT MAX(id) FROM " + (isPrecio ? "precios" : "embarcadero");
        PreparedStatement selectStatement = connection.prepareStatement(selectSql);
        ResultSet resultSet = selectStatement.executeQuery();
        resultSet.next(); // Nos movemos a la primera fila
        return resultSet.getInt(1);
    }

    private static Localidad selectLocalidad(Connection connection, String localidad) throws SQLException {
        PreparedStatement selectLocalidad = connection.prepareStatement("select *\n" +
                "from localidad\n" +
                "where nombre = ?;\n");
        selectLocalidad.setString(1, localidad);
        ResultSet localidadFounded = selectLocalidad.executeQuery();        
        if(localidadFounded.next()){
            Localidad localidadToReturn = new Localidad(localidadFounded.getInt("id"),
                    localidadFounded.getString("provincia"),
                    localidadFounded.getString("municipio"),
                    localidadFounded.getString("nombre"));
         return localidadToReturn;
         }else return null;
    }

    private static Empresa selectEmpresa(Connection connection, String empresa) throws SQLException {
        PreparedStatement selectEmpresa = connection.prepareStatement("select *\n" +
                "from empresa\n" +
                "where nombre = ?;\n");
        selectEmpresa.setString(1, empresa);
        ResultSet empresaFounded = selectEmpresa.executeQuery();
        if(empresaFounded.next()){
            Empresa empresaToReturn = new Empresa(empresaFounded.getInt("id"),
                empresaFounded.getString("nombre"),
                empresaFounded.getString("direccion"),
                empresaFounded.getInt("telefono"));
         return empresaToReturn;
         }else return null;
    }

    private static float parseFloat(String number){
        float result = number != null && number.length() > 0 ? Float.parseFloat(number.replace(",", ".")) : -1;
        return result;
    }
}
