package org.harry.mlfqe.interactors.exasol;

import org.apache.commons.lang3.StringUtils;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;
import org.javatuples.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class ExasolInteractor implements Interactor {

    public JDBCProperties jdbcProperties;
    public String systemName;
    public ArrayList<String> registeredSystems;
    public HashMap<String, String> registeredTables;
    public ArrayList<String> registeredViews;
    public ArrayList<String> registeredJoinViews;


    public ExasolInteractor(String systemName) {

        this.systemName = systemName;
        this.jdbcProperties = new JDBCProperties();
        this.registeredSystems = new ArrayList<>();
        this.registeredTables = new HashMap<>();
        this.registeredViews = new ArrayList<>();
        this.registeredJoinViews = new ArrayList<>();

    }

    @Override
    public String getSystemName() {
        return systemName;
    }

    @Override
    public HashMap<String, Long> getAttributes(String tableName) {
        return null;
    }

    @Override
    public Long getTableSize(String tableName) {
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredViews() {
        return this.registeredViews;
    }

    @Override
    public ArrayList<String> getRegisteredJoinViews() {
        return this.registeredJoinViews;
    }

    @Override
    public ArrayList<String> getRegisteredTables() {
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredForeignTables() {
        return null;
    }


    @Override
    public void cleanUp() {
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            for (String view : this.registeredViews) {
                stmt.execute("DROP VIEW IF EXISTS " + view + " CASCADE");
            }
            for (String table : this.registeredTables.keySet()) {
                stmt.execute("DROP VIRTUAL SCHEMA IF EXISTS " + this.registeredTables.get(table) + " CASCADE");
            }
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    public void initialize(String propertiesFile) {

        try (InputStream input = new FileInputStream(propertiesFile)) {

            Properties prop = new Properties();
            prop.load(input);

            this.jdbcProperties.setSystemName(this.systemName);
            this.jdbcProperties.setPassword(prop.getProperty("password"));
            this.jdbcProperties.setUser(prop.getProperty("user"));
            this.jdbcProperties.setUrl(prop.getProperty("url"));
            this.jdbcProperties.setDriverName(prop.getProperty("driverName"));
            this.jdbcProperties.setDriverJar(prop.getProperty("driverJar"));
            this.jdbcProperties.setOdbcDriver(prop.getProperty("odbcDriver", ""));

            instantiateWrapper();

        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }

    public void instantiateWrapper() {

        //TODO: check if necessary to split
        String crt = "CREATE SCHEMA IF NOT EXISTS ADAPTER";
        String crt2 = "CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER_PG AS\n" +
                "%scriptclass com.exasol.adapter.RequestDispatcher;\n" +
                "%jar /buckets/bfsdefault/default/virtualschemas/virtual-schema-dist-9.0.1-postgresql-2.0.0.jar;\n" +
                "%jar /buckets/bfsdefault/default/drivers/jdbc/postgres/postgresql-42.2.18.jar;\n" +
                "/";

        String crt3 = "CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER_MDB AS\n" +
                "%scriptclass com.exasol.adapter.RequestDispatcher;\n" +
                "%jar /buckets/bfsdefault/default/virtualschemas/virtual-schema-dist-9.0.1-mysql-2.0.0.jar;\n" +
                "%jar /buckets/bfsdefault/default/drivers/jdbc/mariadb/mariadb-java-client-2.7.1.jar;\n" +
                "/";

        String crt4 = "CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER_HIVE AS\n" +
                "%scriptclass com.exasol.adapter.RequestDispatcher;\n" +
                //"%jar /buckets/bfsdefault/default/virtualschemas/virtual-schema-dist-9.0.3-generic-2.0.0.jar;\n" +
                "%jar /buckets/bfsdefault/default/virtualschemas/virtual-schema-dist-9.0.1-hive-2.0.0.jar;\n" +
                //"%jar /buckets/bfsdefault/default/virtualschemas/virtual-schema-dist-9.0.3-exasol-5.0.4.jar;\n" +
                "%jar /buckets/bfsdefault/default/drivers/jdbc/hive/HiveJDBC41.jar;\n" +
                "/";
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.execute(crt);
            stmt.execute(crt2);
            stmt.execute(crt3);
            stmt.execute(crt4);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }


    @Override
    public void registerLocalView(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS \"" + viewName + "\"";

        String localView = "CREATE VIEW \"" + viewName + "\" AS " + query;

        localView = replaceRegTables(localView);

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            //System.out.println(dropView);
            //System.out.println(localView);
            stmt.execute(dropView);
            stmt.execute(localView);
            stmt.close();
            connection.close();
            this.registeredViews.add(viewName);


        } catch (SQLException throwables) {
            System.out.println("Exception in " + this.getSystemName());
            System.out.println("Query: " + localView);
            throwables.printStackTrace();
        }
    }

    @Override
    public void registerJoinView(String viewName, String tableA, String tableB, String joinOn) {

        String dropView = "DROP VIEW IF EXISTS \"" + viewName + "\"";

        String joinView = "CREATE VIEW \"" + viewName + "\" AS SELECT * FROM " + tableA + "," + tableB + " WHERE " + joinOn;

        joinView = replaceRegTables(joinView);

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            //System.out.println(dropView);
            //System.out.println(joinView);
            stmt.execute(dropView);
            stmt.execute(joinView);
            connection.close();
            this.registeredViews.add(viewName);
            this.registeredJoinViews.add(viewName);

        } catch (SQLException throwables) {
            System.out.println(joinView);
            throwables.printStackTrace();
        }


    }

    @Override
    public void registerForeignTable(SystemCatalog sc, String foreignSystemName, String tableName) {

        ForeignSystemHandler fsh;
        /*if (foreignSystemName.contains("exa"))
            fsh = new ExasolSystemHandler();
        else*/
        fsh = new JDBCSystemHandler();

        //TODO: add table filter, as shown in https://docs.exasol.com/database_concepts/virtual_schema/adapter_properties.htm
        fsh.registerForeignSystem(this, sc.get(foreignSystemName).getJDBCProperties());
        //System.out.println(this.registeredSystems);
        //System.out.println("system: " + foreignSystemName);
        if (!this.registeredSystems.contains(foreignSystemName)) {

            //System.out.println("is not contained");
            this.registeredSystems.add(foreignSystemName);
        }
        this.registeredTables.put(tableName, foreignSystemName);

        //fsh.registerForeignTable(this, sc.get(foreignSystemName).getJDBCProperties(), tableName, foreignSystemName);

    }

    @Override
    public boolean registerLocalMaterializedView(String viewName, String query) {

        String dropView = "DROP TABLE IF EXISTS " + viewName + " CASCADE";
        String localView = "CREATE TABLE " + viewName + " AS " + query;
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            stmt.executeUpdate(dropView);
            stmt.executeUpdate(localView);

            stmt.close();
            connection.close();
            this.registeredTables.put(viewName, "");
            return true;
        } catch (SQLException throwables) {
            System.out.println("Exception for query: " + localView);
            throwables.printStackTrace();
        }
        return false;
    }

    @Override
    public void executeQuery(String query) {

    }

    public String replaceRegTables(String query) {

        //System.out.println("INPUT q:" + query);
        String tables = "";
        if (query.contains("DROP"))
            tables = Utils.subStrBetween(query, "EXISTS ", " CASCADE");
        else if (query.toLowerCase(Locale.ROOT).contains("where") && query.toLowerCase(Locale.ROOT).contains("  from"))
            tables = Utils.subStrBetween(query.toLowerCase(Locale.ROOT), "  from ", " where");
        else if (query.toLowerCase(Locale.ROOT).contains("where"))
            tables = Utils.subStrBetween(query.toLowerCase(Locale.ROOT), "from ", " where");
        else
            tables = query.substring(query.toLowerCase(Locale.ROOT).indexOf("from ") + 5);

        String newTables = "";
        String delimiter = "";

        try {


            for (String tbl : tables.split(",")) {
                if (this.registeredTables.containsKey(Utils.sanitize(tbl))) {
                    String schema = this.registeredTables.get(Utils.sanitize(tbl)) + "_schema";

                    //newTables += delimiter + schema + ".\"" + Utils.sanitize(tbl) + "\"";
                    newTables += delimiter + schema + "." + Utils.sanitize(tbl);

                } else {
                    newTables += delimiter + "\"" + Utils.sanitize(tbl) + "\"";

                }
                delimiter = ",";
            }

        } catch (Exception e) {
            System.out.println(query);
            e.printStackTrace();
        }
        String retQuery = query.replace(tables, newTables);
        //System.out.println("RETQUERY:" + retQuery);

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();


            String cols = Arrays.stream(tables.split(",")).map(s -> "'" + s + "'" + ",'" + s.toUpperCase(Locale.ROOT) + "'").collect(Collectors.joining(","));
            String q = "SELECT COLUMN_SCHEMA, COLUMN_NAME FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_TABLE IN(" + cols + ")";
            if (this.registeredTables.size() > 0)
                q += " AND COLUMN_SCHEMA IN (" + this.registeredTables.values().stream().map(s -> "'" + s.toUpperCase(Locale.ROOT) + "_SCHEMA'").collect(Collectors.joining(",")) + ",'" + this.jdbcProperties.getDatabaseName() + "')";
            //System.out.println(q);
            ResultSet rs = stmt.executeQuery(q);

            while (rs.next()) {
                //System.out.println(rs.getString(2) + " is all lower case: " + StringUtils.isAllLowerCase(rs.getString(2).replace("_", "").replaceAll("[0-9]", "")));
                if (retQuery.contains(rs.getString(2).toLowerCase(Locale.ROOT)) && StringUtils.isAllLowerCase(rs.getString(2).replace("_", "").replaceAll("[0-9]", ""))) {
                    //if (retQuery.contains(rs.getString(2).toLowerCase(Locale.ROOT)) && !rs.getString(1).toLowerCase(Locale.ROOT).contains("_schema")) {
                    //System.out.println("REPLACING " + rs.getString(2) + " WITH " + "\"" + rs.getString(2) + "\"");
                    retQuery = retQuery.replaceAll(rs.getString(2), "\"" + rs.getString(2) + "\"");
                }
            }
        } catch (SQLException throwables) {
            System.out.println(retQuery);
            throwables.printStackTrace();
        }


        return retQuery.replaceAll("\"\"", "\"");
    }

    public String getQualifiedViewName(String viewName) {

        return viewName;
    }


    @Override
    public JDBCProperties getJDBCProperties() {
        return this.jdbcProperties;
    }

    @Override
    public void executeQueryAndPrintResult(String query) throws ClassNotFoundException {

        try {
            //Class.forName(this.jdbcProperties.getDriverName());

            String newQuery = replaceRegTables(query);

            System.out.println("------------------------------------------------------------------------");
            System.out.println(this.systemName + " Executing query: ");
            System.out.println(newQuery);
            System.out.println("------------------------------------------------------------------------");

            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            stmt.executeUpdate("ALTER SESSION SET query_cache='OFF'");
            //stmt.setQueryTimeout(300);
            stmt.executeUpdate("ALTER SESSION SET QUERY_TIMEOUT=300");
            if (query.toLowerCase(Locale.ROOT).contains("select")) {
                ResultSet rs = stmt.executeQuery(newQuery);
                Utils.printResultSet(rs);
            } else
                stmt.execute(newQuery);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

    @Override
    public long getQueryCost(String query) {
        return 0;
    }

    @Override
    public void createDummyTable(String tableName, Relation r) {

    }

    @Override
    public void updateStatistics(String tableName, Relation r, boolean analyze) {

    }

    @Override
    public Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException {
        ResultSet rs;
        Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

        Statement stmt = connection.createStatement();

        rs = stmt.executeQuery(query);
        Pair<Connection, ResultSet> ret = new Pair(connection, rs);

        return ret;
    }
}
