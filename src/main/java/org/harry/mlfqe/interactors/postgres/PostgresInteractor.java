package org.harry.mlfqe.interactors.postgres;

import org.apache.commons.io.FileUtils;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Attribute;
import org.harry.mlfqe.core.optimizer.Relation;
import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

public class PostgresInteractor implements Interactor {

    public JDBCProperties jdbcProperties;
    public String systemName;
    public ArrayList<String> registeredSystems;
    public ArrayList<String> registeredViews;
    public ArrayList<String> registeredJoinViews;
    public ArrayList<String> registeredTables;
    public ArrayList<String> registeredForeignTables;

    public PostgresInteractor(String systemName) {

        this.systemName = systemName;
        this.jdbcProperties = new JDBCProperties();
        this.registeredSystems = new ArrayList<>();
        this.registeredViews = new ArrayList<>();
        this.registeredJoinViews = new ArrayList<>();
        this.registeredTables = new ArrayList<>();
        this.registeredForeignTables = new ArrayList<>();
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

        Long rows = 0L;
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            String sqlQuery = "SELECT reltuples FROM pg_class WHERE relname='" + tableName + "'";
            Statement stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery(sqlQuery);
            rs.next();
            rows = rs.getLong(1);
            rs.close();
            stmt.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        return rows;

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
        return this.registeredTables;
    }

    @Override
    public ArrayList<String> getRegisteredForeignTables() {
        return this.registeredForeignTables;
    }

    @Override
    public void cleanUp() {
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            for (String view : this.registeredViews) {
                stmt.executeUpdate("DROP VIEW IF EXISTS " + view + " CASCADE");
            }
            for (String view : this.registeredJoinViews) {
                stmt.executeUpdate("DROP VIEW IF EXISTS " + view + " CASCADE");
            }
            for (String table : this.registeredTables) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + table + " CASCADE");
            }
            for (String table : this.registeredForeignTables) {
                stmt.executeUpdate("DROP FOREIGN TABLE IF EXISTS " + table + " CASCADE");
            }

            this.registeredViews = new ArrayList<>();
            this.registeredJoinViews = new ArrayList<>();
            this.registeredTables = new ArrayList<>();
            this.registeredForeignTables = new ArrayList<>();

            stmt.close();
            connection.close();


        } catch (SQLException throwables) {
            System.out.println("Exception for interactor: " + this.systemName);
            throwables.printStackTrace();
        }

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

    @Override
    public void registerLocalView(String viewName, String query) {
        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";
        String localView = "CREATE VIEW " + viewName + " AS " + query;
        System.out.println(this.systemName + ": " + localView);
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            stmt.executeUpdate(dropView);
            stmt.executeUpdate(localView);

            stmt.close();
            connection.close();
            this.registeredViews.add(viewName);
        } catch (SQLException throwables) {
            System.out.println("Exception for query: " + localView);
            throwables.printStackTrace();
        }

    }

    public void registerFakeView(String viewName, String function, String matView) {
        String funcName = viewName + "_func";
        String dropFunc = "DROP FUNCTION IF EXISTS " + funcName + "(text) CASCADE";
        String regView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + funcName + "('" + matView + "')";
        System.out.println(this.systemName + ": " + regView);

        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();

            stmt.executeUpdate(dropFunc);
            stmt.executeUpdate(function);
            stmt.executeUpdate(regView);

            stmt.close();
            connection.close();
            this.registeredViews.add(viewName);

        } catch (SQLException throwables) {
            System.out.println("Exception for query: " + viewName);
            throwables.printStackTrace();
        }


    }

    public boolean registerLocalMaterializedView(String viewName, String query) {
        String dropView = "DROP TABLE IF EXISTS " + viewName + " CASCADE";
        String localView = "CREATE TABLE " + viewName + " AS " + query;
        System.out.println(this.systemName + ": " + localView);
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
    public void registerJoinView(String viewName, String tableA, String tableB, String joinOn) {

        String dropView = "DROP VIEW IF EXISTS " + viewName + " CASCADE";
        String joinView = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableA + ", " + tableB + " WHERE " + joinOn;

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
    public void registerForeignTable(SystemCatalog sc, String foreignSystemName, String tableName) {

        ForeignSystemHandler fsh;
        if (sc.get(foreignSystemName).getJDBCProperties().getUrl().contains("postgres"))
            fsh = new PostgresSystemHandler();
        else if (sc.get(foreignSystemName).getJDBCProperties().getUrl().contains("exa"))
            fsh = new JDBCSystemHandler();
        else
            fsh = new ODBCSystemHandler();

        fsh.createExtension(this.jdbcProperties);

        if (!this.registeredSystems.contains(foreignSystemName)) {
            fsh.registerForeignSystem(this, sc.get(foreignSystemName).getJDBCProperties());
            this.registeredSystems.add(foreignSystemName);
        }

        fsh.registerForeignTable(this, sc.get(foreignSystemName).getJDBCProperties(), tableName, foreignSystemName);
        this.registeredForeignTables.add(tableName);

    }


    @Override
    public JDBCProperties getJDBCProperties() {
        return this.jdbcProperties;
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

            int timeOut = 3600;

            stmt.setQueryTimeout(timeOut);
            //stmt.execute("SET statement_timeout=240000");
            stmt.execute("SET enable_nestloop=off;SET enable_mergejoin=off;");
            //stmt.execute("SET from_collapse_limit=1;SET join_collapse_limit=1;");
            if (query.toLowerCase(Locale.ROOT).contains("select")) {
                ResultSet rs = stmt.executeQuery(query);

            } else
                stmt.execute(query);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }


    }

    @Override
    public void executeQueryAndPrintResult(String query) {

        try {
            //Class.forName(this.jdbcProperties.getDriverName());
            System.out.println("------------------------------------------------------------------------");
            System.out.println(this.systemName + " Executing query: ");
            System.out.println(query);
            System.out.println("------------------------------------------------------------------------");

            //System.out.println(this.getExplain(query));
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            int timeOut = 3600;
            /*if (System.getProperties().containsKey("sf") && Integer.parseInt(System.getProperty("sf")) > 1)
                timeOut = 60 * Integer.parseInt(System.getProperty("sf")) / 2;*/

            stmt.setQueryTimeout(timeOut);
            stmt.execute("SET enable_nestloop=off;");
            //stmt.execute("SET enable_mergejoin=off;");
            //stmt.execute("SET from_collapse_limit=1;SET join_collapse_limit=1;");

            if (System.getProperties().containsKey("parallel") && System.getProperty("parallel").equals("false"))
                stmt.executeUpdate("SET max_parallel_workers_per_gather = 0;");

            //TODO: remove
            /*try {
                if (System.getProperties().containsKey("explain")) {
                    String td = System.getProperty("td");
                    String sf = System.getProperty("sf");
                    String q = System.getProperty("q");
                    String explain = System.getProperty("explain", "");
                    String ldir = "/tmp/" + System.getProperty("ldir", "");
                    //System.out.println(ldir);

                    ResultSet ers = stmt.executeQuery("EXPLAIN VERBOSE " + query);
                    if (explain.equals("true")) {
                        if (!ldir.equals("/tmp/")) {
                            File file = new File(ldir + "/td" + td + "_sf" + sf + "_q" + q + ".txt");
                            FileUtils.writeStringToFile(file, query, StandardCharsets.UTF_8, true);

                            while (ers.next())
                                FileUtils.writeStringToFile(file, ers.getString(1) + "\n", StandardCharsets.UTF_8, true);
                        } else {
                            while (ers.next())
                                System.out.println(ers.getString(1));
                        }

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }*/


            if (query.toLowerCase(Locale.ROOT).contains("select")) {
                ResultSet rs = stmt.executeQuery(query);
                //ResultSet rs = stmt.executeQuery("SELECT 1");
                Utils.printResultSet(rs);
            } else
                stmt.execute(query);
            connection.close();


        } catch (SQLException throwables) {
            System.out.println("Exception for interactor: " + this.systemName);
            throwables.printStackTrace();
        }


    }

    @Override
    public long getQueryCost(String query) {

        //System.out.println("Getting explain for: " + query);
        String json = getExplain(query);
        JSONObject jsonObject = parseJSON(json);
        return jsonObject.getLong("totalCost");
    }

    @Override
    public void createDummyTable(String tableName, Relation r) {
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            StringBuilder sb = new StringBuilder();


            //TODO: correct types
            String comma = "";
            for (Attribute attr : r.schema) {
                sb.append(comma);
                comma = ",";
                sb.append(attr.name);
                sb.append(" ");
                sb.append("INT");

            }

            String dropStr = "DROP TABLE IF EXISTS " + tableName;
            String createStr = "CREATE TABLE " + tableName + "(" + sb + ")";
            //System.out.println(createStr);

            stmt.execute(dropStr);
            stmt.execute(createStr);
            this.registeredTables.add(tableName);
            updateStatistics(tableName, r, false);
            connection.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void analyze(String tableName) {
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();
            stmt.execute("analyze " + tableName);
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateStatistics(String tableName, Relation r, boolean analyze) {
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());
            Statement stmt = connection.createStatement();


            if (analyze)
                stmt.execute("analyze " + r.shortName);
            else {
                //TODO: correct replages size
                double relPages = r.rowCount / 35;
                //relPages = 0;
                String sqlQuery = "UPDATE pg_class SET reltuples=" + r.rowCount + ",relpages=" + relPages + " where relname='" + r.shortName + "'";
                //System.out.println("Updating statistics: " + sqlQuery);
                stmt.execute(sqlQuery);


                //TODO: update attribute stats
                StringBuilder attrQuery = new StringBuilder("ALTER TABLE " + r.shortName + " ");
                String sep = "";
                for (Attribute attr : r.schema) {
                    if (attr.distinctVals > 10) {
                        attrQuery.append(sep)
                                .append(" ALTER COLUMN ")
                                .append(attr.name)
                                .append(" SET(n_distinct=")
                                .append(attr.distinctVals)
                                .append(", n_distinct_inherited=")
                                .append(attr.distinctVals)
                                .append(")");

                        sep = ",";
                    }
                }
                //System.out.println(attrQuery);
                if (sep.equals(","))
                    stmt.execute(attrQuery.toString());
            /*ResultSet rs = stmt.executeQuery("SELECT oid FROM pg_class WHERE relname = '"+r.shortName+"'");
            rs.next();
            int oid = rs.getInt(1);*/
            }
            stmt.close();
            connection.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException {
        ResultSet rs;

        Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

        Statement stmt = connection.createStatement();
        stmt.execute("SET enable_nestloop=off;SET enable_mergejoin=off;");
        //stmt.execute("SET from_collapse_limit=1;SET join_collapse_limit=1;");

        //System.out.println(this.systemName + ": " + query);
        rs = stmt.executeQuery(query);
        Pair<Connection, ResultSet> ret = new Pair(connection, rs);

        return ret;
    }

    public long executeLogQuery(String query, String output) throws JSONException, SQLException, IOException {

        // Execute and log execution time
        //System.out.println(query);
        long start = System.nanoTime();
        executeQuery(query);
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

        return executionTime;

    }

    public String getExplain(String query) {

        StringBuilder sb = new StringBuilder();
        try {
            Connection connection = DriverManager.getConnection(this.jdbcProperties.getUrl(),
                    this.jdbcProperties.getUser(), this.jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate("SET enable_nestloop='off'");
            //stmt.executeUpdate("SET enable_mergejoin='off'");
            ResultSet rs = stmt.executeQuery("EXPLAIN (FORMAT JSON)" + query);


            ResultSetMetaData metaData = rs.getMetaData();
            int nColumns = metaData.getColumnCount();
            for (int i = 1; i <= nColumns; ++i) {
                while (rs.next()) {
                    sb.append(rs.getString(i));
                }
            }
            rs.close();
            stmt.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public JSONObject parseJSON(String explainStmnt) {
        //Transform the explain statement to JSONArray with a single element
        JSONArray dataArray = new JSONArray(explainStmnt);
        JSONObject metaData = dataArray.getJSONObject(0);
        JSONObject plan = metaData.getJSONObject("Plan");
        //Only keep Node Type (i.e. Join Type) and Total Cost
        JSONObject result = new JSONObject();
        result.put("type", plan.get("Node Type"));
        result.put("totalCost", plan.get("Total Cost"));

        return result;
    }

}
