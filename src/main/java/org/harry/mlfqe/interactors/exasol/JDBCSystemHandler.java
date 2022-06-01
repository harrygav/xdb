package org.harry.mlfqe.interactors.exasol;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCSystemHandler implements ForeignSystemHandler {
    public JDBCSystemHandler() {
    }

    @Override
    public void createExtension(JDBCProperties jdbcProperties) {

    }

    @Override
    public void registerForeignSystem(Interactor interactor, JDBCProperties fjdbcProperties) {

        String schemaName = fjdbcProperties.getSystemName() + "_schema";
        String connName = fjdbcProperties.getSystemName() + "_CONNECTION";
        String schemaDrop = "drop virtual schema if exists " + schemaName + " cascade";

        String connectionUrl = fjdbcProperties.getUrl();
        if (fjdbcProperties.getSystemName().contains("exa"))
            connectionUrl += ";validateservercertificate=0";

        String connReg = "CREATE OR REPLACE CONNECTION " + connName + "\n " +
                "TO '" + connectionUrl + "'\n" +
                "USER '" + fjdbcProperties.getUser() + "'\n" +
                "IDENTIFIED BY '" + fjdbcProperties.getPassword() + "'";

        String fSchemaName = "";
        String cName = "CATALOG_NAME = '" + fjdbcProperties.getDatabaseName() + "'\n";
        if (fjdbcProperties.getDriverName().contains("postgres"))
            fSchemaName += "SCHEMA_NAME='public'\n";
        if (fjdbcProperties.getDriverName().contains("hive")) {
            fSchemaName += "SCHEMA_NAME='" + fjdbcProperties.getDatabaseName() + "'";
            cName = "";
        }

        //TODO: check if necessary see ExasolInteractor instantiateWrapper()
        String adapter = "";

        if (fjdbcProperties.getUrl().contains("postgres"))
            adapter = "_PG";
        else if (fjdbcProperties.getUrl().contains("mariadb"))
            adapter = "_MDB";
        else if (fjdbcProperties.getUrl().contains("hive"))
            adapter = "_HIVE";

        String schemaReg = "CREATE VIRTUAL SCHEMA " + schemaName + "\n" +
                "USING ADAPTER.JDBC_ADAPTER" + adapter + "\n" +
                "WITH\n" +
                cName +
                fSchemaName +
                "CONNECTION_NAME = '" + connName + "'";
        try {

            //System.out.println(schemaReg);

            Connection connection = DriverManager.getConnection(interactor.getJDBCProperties().getUrl(),
                    interactor.getJDBCProperties().getUser(), interactor.getJDBCProperties().getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(schemaDrop);
            stmt.executeUpdate(connReg);
            stmt.executeUpdate(schemaReg);

            stmt.close();
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void registerForeignTable(Interactor interactor, JDBCProperties fjdbcProperties, String
            tableName, String foreignSystemName) {

        String tblDrop = "DROP TABLE IF EXISTS " + tableName;

        String createTable = "";

        //System.out.println(createTable);

        try {
            //Class.forName(this.jdbcProperties.getDriverName());

            Connection connection = DriverManager.getConnection(interactor.getJDBCProperties().getUrl(),
                    interactor.getJDBCProperties().getUser(), interactor.getJDBCProperties().getPassword());

            Statement stmt = connection.createStatement();

            stmt.execute(tblDrop);
            stmt.execute(createTable);


            connection.close();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
