package org.harry.mlfqe.core;

import org.harry.mlfqe.core.optimizer.Relation;
import org.javatuples.Pair;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public interface Interactor {

    void initialize(String propertiesFile);

    void registerLocalView(String viewName, String query);

    void registerJoinView(String viewName, String tableA, String tableB, String joinOn);

    void registerForeignTable(SystemCatalog sc, String systemName, String tableName);

    boolean registerLocalMaterializedView(String viewName, String query);

    void executeQuery(String query);

    void executeQueryAndPrintResult(String query) throws ClassNotFoundException;

    long getQueryCost(String query);

    void createDummyTable(String tableName, Relation r);

    void updateStatistics(String tableName, Relation r, boolean analyze);

    Pair<Connection, ResultSet> executeQueryAndReturnRS(String query) throws SQLException;

    JDBCProperties getJDBCProperties();

    String getSystemName();

    HashMap<String, Long> getAttributes(String tableName);

    Long getTableSize(String tableName);

    //TODO: merge views with joinviews
    ArrayList<String> getRegisteredViews();

    ArrayList<String> getRegisteredJoinViews();

    ArrayList<String> getRegisteredTables();

    ArrayList<String> getRegisteredForeignTables();

    void cleanUp();

}
