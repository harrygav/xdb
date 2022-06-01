package org.harry.mlfqe.interactors.mariadb;

import org.apache.commons.io.FileUtils;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;
import org.harry.mlfqe.interactors.postgres.PostgresSystemHandler;
import org.javatuples.Pair;
import org.json.JSONException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

public class MariaDBInteractor implements Interactor {

    public JDBCProperties jdbcProperties;
    public String systemName;
    public ArrayList<String> registeredSystems;
    public ArrayList<String> registeredViews;
    public ArrayList<String> registeredJoinViews;
    public ArrayList<String> registeredTables;

    public MariaDBInteractor(String systemName) {

        this.systemName = systemName;
        this.jdbcProperties = new JDBCProperties();
        this.registeredSystems = new ArrayList<>();
        this.registeredViews = new ArrayList<>();
        this.registeredJoinViews = new ArrayList<>();
        this.registeredTables = new ArrayList<>();
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
        return registeredTables;
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
            for (String table : this.registeredTables) {
                stmt.execute("DROP TABLE IF EXISTS " + table + " CASCADE");
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
        String fdw = "INSTALL SONAME 'ha_connect';";
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.execute(fdw);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    @Override
    public void registerLocalView(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String joinView = "CREATE VIEW " + viewName + " AS " + query;

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(dropView);
            stmt.executeUpdate(joinView);
            stmt.close();
            connection.close();
            this.registeredViews.add(viewName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    @Override
    public void registerJoinView(String viewName, String tableA, String tableB, String joinOn) {

        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableA + "," + tableB + " WHERE " + joinOn;

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(dropView);
            stmt.executeUpdate(joinView);
            stmt.close();
            connection.close();
            this.registeredViews.add(viewName);
            this.registeredJoinViews.add(viewName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

    @Override
    public void registerForeignTable(SystemCatalog sc, String foreignSystemName, String tableName) {

        ForeignSystemHandler fsh;
        if (sc.get(foreignSystemName).getJDBCProperties().getUrl().contains("mariadb"))
            fsh = new MariaDBSystemHandler();
        else if (sc.get(foreignSystemName).getJDBCProperties().getUrl().contains("exa"))
            fsh = new org.harry.mlfqe.interactors.mariadb.JDBCSystemHandler();
        else {
            //fsh = new org.harry.mlfqe.interactors.mariadb.JDBCSystemHandler();
            fsh = new org.harry.mlfqe.interactors.mariadb.ODBCSystemHandler();
        }

        fsh.createExtension(this.jdbcProperties);

        //if (!this.registeredSystems.contains(foreignSystemName))
        fsh.registerForeignSystem(this, sc.get(foreignSystemName).getJDBCProperties());

        fsh.registerForeignTable(this, sc.get(foreignSystemName).getJDBCProperties(), tableName, foreignSystemName);
        this.registeredTables.add(tableName);
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
            this.registeredTables.add(viewName);
            return true;
        } catch (SQLException throwables) {
            System.out.println("Exception for query: " + localView);
            throwables.printStackTrace();
        }
        return false;
    }


    @Override
    public JDBCProperties getJDBCProperties() {
        return this.jdbcProperties;
    }

    @Override
    public void executeQueryAndPrintResult(String query) throws ClassNotFoundException {

        try {
            //Class.forName(this.jdbcProperties.getDriverName());
            System.out.println("------------------------------------------------------------------------");
            System.out.println(this.systemName + " Executing query: ");
            System.out.println(query);
            System.out.println("------------------------------------------------------------------------");

            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            stmt.setQueryTimeout(300);
            stmt.execute("SET SESSION join_cache_level=8");
            stmt.execute("SET SESSION optimizer_switch='mrr=on'");
            stmt.execute("SET SESSION optimizer_switch='mrr_sort_keys=on'");
            stmt.execute("SET SESSION connect_work_size=1073741824");

            if (query.toLowerCase(Locale.ROOT).contains("select")) {
                ResultSet rs = stmt.executeQuery(query);
                Utils.printResultSet(rs);
            } else
                stmt.execute(query);
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


    public void executeLogQuery(String query, String output) throws ClassNotFoundException, IOException, JSONException, SQLException {

        // Execute and log execution time
        try {
            //Class.forName(this.jdbcProperties.getDriverName());
            System.out.println("------------------------------------------------------------------------");
            System.out.println(this.systemName + " Executing query: ");
            System.out.println(query);
            System.out.println("------------------------------------------------------------------------");

            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            stmt.execute("SET SESSION join_cache_level=8;");
            stmt.execute("SET SESSION optimizer_switch='mrr=on';");
            stmt.execute("SET SESSION optimizer_switch='mrr_sort_keys=on';");


            if (query.toLowerCase(Locale.ROOT).contains("select")) {
                long start = System.nanoTime();
                stmt.executeQuery(query);
                long end = System.nanoTime();
                long executionTime = end - start;

                String countQuery = query.replace("*", "COUNT(*)");
                Pair<Connection, ResultSet> ret = executeQueryAndReturnRS(countQuery);
                ResultSet rs = ret.getValue1();
                ResultSetMetaData rsmd = rs.getMetaData();
                StringBuilder sb = new StringBuilder();
                while (rs.next()) {
                    for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
                        if (i != 1)
                            sb.append("|");
                        sb.append(rs.getString(i));
                    }
                }
                String count = sb.toString();

                String dataPoint = query + "time:" + executionTime + ";count:" + count + ";\n";
                FileUtils.writeStringToFile(new File(output), dataPoint, StandardCharsets.UTF_8, true);

            } else {
                stmt.execute(query);
            }
            connection.close();
            //executeQueryAndPrintResult(query);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return;

    }

    public void executeQuery(String query) {

        try {
            //Class.forName(this.jdbcProperties.getDriverName());
            System.out.println("------------------------------------------------------------------------");
            System.out.println(this.systemName + " Executing query: ");
            System.out.println(query);
            System.out.println("------------------------------------------------------------------------");

            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            stmt.setQueryTimeout(300);
            stmt.execute("SET SESSION join_cache_level=8");
            stmt.execute("SET SESSION optimizer_switch='mrr=on'");
            stmt.execute("SET SESSION optimizer_switch='mrr_sort_keys=on'");
            stmt.execute("SET SESSION connect_work_size=1073741824");


            stmt.execute(query);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }


}
