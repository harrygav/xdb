package org.harry.mlfqe.interactors.dummy;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.core.optimizer.bottomup.BottomUpOptimizer;
import org.javatuples.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class DummyInteractor implements Interactor {

    public JDBCProperties jdbcProperties;
    public String systemName;
    public ArrayList<String> registeredViews;
    public ArrayList<String> registeredJoinViews;

    public DummyInteractor(String systemName) {

        this.systemName = systemName;
        this.jdbcProperties = new JDBCProperties();
        this.registeredViews = new ArrayList<>();
        this.registeredJoinViews = new ArrayList<>();
    }

    public void initialize(String propertiesFile) {

        try (InputStream input = new FileInputStream(propertiesFile)) {

            Properties prop = new Properties();
            prop.load(input);

            this.jdbcProperties.setSystemName(this.systemName);
            this.jdbcProperties.setPassword(prop.getProperty("password", ""));
            this.jdbcProperties.setUser(prop.getProperty("user", ""));
            this.jdbcProperties.setUrl(prop.getProperty("url", ""));
            this.jdbcProperties.setDriverName(prop.getProperty("driverName", ""));
            this.jdbcProperties.setDriverJar(prop.getProperty("driverJar", ""));
            this.jdbcProperties.setOdbcDriver(prop.getProperty("odbcDriver", ""));

            //instantiateWrapper();

        } catch (IOException ex) {
            ex.printStackTrace();
        }


    }

    public void loadTables(String createSql) {
        try {
            Connection con = DriverManager.getConnection(this.jdbcProperties.getUrl(), this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            String create = Utils.getResourceAsString(BottomUpOptimizer.class.getClassLoader().getResourceAsStream(createSql));
            String views = Utils.getResourceAsString(BottomUpOptimizer.class.getClassLoader().getResourceAsStream("tpchq/create_tpch_views.sql"));
            Statement stmt = con.createStatement();
            stmt.execute(create);
            stmt.execute(views);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void registerLocalView(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";
        String localView = "CREATE VIEW " + viewName + " AS " + query;
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            stmt.executeUpdate(dropView);
            stmt.executeUpdate(localView);

            stmt.close();
            connection.close();

        } catch (SQLException throwables) {
            System.out.println("Exception for query: " + localView);
            throwables.printStackTrace();
        }

    }

    @Override
    public void registerJoinView(String viewName, String tableA, String tableB, String joinOn) {

        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";
        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableA + "," + tableB + " WHERE " + joinOn;

        System.out.println(this.systemName + ": " + joinView);

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
    public void registerForeignTable(SystemCatalog sc, String systemName, String tableName) {

    }

    @Override
    public boolean registerLocalMaterializedView(String viewName, String query) {
        return false;
    }

    @Override
    public void executeQuery(String query) {

    }

    public void execute(String query) throws ClassNotFoundException {

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            stmt.execute(query);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeQueryAndPrintResult(String query) throws ClassNotFoundException {

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            Utils.printResultSet(rs);

        } catch (SQLException e) {
            e.printStackTrace();
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
        return null;
    }

    @Override
    public JDBCProperties getJDBCProperties() {
        return this.jdbcProperties;
    }

    @Override
    public String getSystemName() {
        return this.systemName;
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
        return null;
    }

    @Override
    public ArrayList<String> getRegisteredJoinViews() {
        return null;
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

    }
}
