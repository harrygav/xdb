package org.harry.mlfqe.interactors.hive;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Relation;
import org.javatuples.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

public class HiveInteractor implements Interactor {

    public JDBCProperties jdbcProperties;
    public String systemName;
    public ArrayList<String> registeredSystems;
    public ArrayList<String> registeredViews;
    public ArrayList<String> registeredJoinViews;
    public ArrayList<String> registeredTables;

    public HiveInteractor(String systemName) {

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
                stmt.execute("DROP VIEW IF EXISTS " + view);
            }
            for (String table : this.registeredTables) {
                stmt.execute("DROP TABLE IF EXISTS " + table);
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

    }


    @Override
    public void registerLocalView(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName;

        String localView = "CREATE VIEW " + viewName + " AS " + query;

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            //System.out.println(dropView);
            //System.out.println(joinView);
            stmt.execute(dropView);
            stmt.execute(localView);
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
            //System.out.println(dropView);
            //System.out.println(joinView);
            stmt.execute(dropView);
            stmt.execute(joinView);
            connection.close();
            this.registeredViews.add(viewName);
            this.registeredJoinViews.add(viewName);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

    @Override
    public void registerForeignTable(SystemCatalog sc, String foreignSystemName, String tableName) {


        JDBCSystemHandler jdbcSystemHandler = new JDBCSystemHandler();

        jdbcSystemHandler.registerForeignTable(this, sc.get(foreignSystemName).getJDBCProperties(), tableName, foreignSystemName);
        this.registeredTables.add(tableName);


    }

    @Override
    public boolean registerLocalMaterializedView(String viewName, String query) {
        return false;
    }

    @Override
    public void executeQuery(String query) {

    }


    @Override
    public JDBCProperties getJDBCProperties() {
        return this.jdbcProperties;
    }

    @Override
    public void executeQueryAndPrintResult(String query) throws ClassNotFoundException {

        try {
            //Class.forName(this.jdbcProperties.getDriverName());

            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.setQueryTimeout(300);
            stmt.execute("SET hive.execution.engine=mr");
            stmt.execute("SET hive.execution.engine=spark");
            stmt.execute("SET mapred.child.java.opts=-Xmx20G -XX:+UseConcMarkSweepGC -XX:-UseGCOverheadLimit -XX:+UseParNewGC");
            stmt.execute("SET hive.exec.reducers.bytes.per.reducer=67108864");
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Executing query on " + this.systemName);
            System.out.println(query);
            System.out.println("------------------------------------------------------------------------");
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


    public Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException {

        ResultSet rs;
        Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

        Statement stmt = connection.createStatement();
        stmt.execute("SET hive.execution.engine=mr");
        stmt.execute("SET hive.execution.engine=spark");
        rs = stmt.executeQuery(query);
        Pair<Connection, ResultSet> ret = new Pair(connection, rs);

        return ret;

    }
}
