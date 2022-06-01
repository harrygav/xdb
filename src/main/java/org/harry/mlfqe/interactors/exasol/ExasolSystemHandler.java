package org.harry.mlfqe.interactors.exasol;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

//TODO: implement correctly, as described in https://github.com/exasol/exasol-virtual-schema/blob/main/doc/dialects/exasol.md
public class ExasolSystemHandler implements ForeignSystemHandler {
    @Override
    public void createExtension(JDBCProperties jdbcProperties) {

    }

    @Override
    public void registerForeignSystem(Interactor interactor, JDBCProperties fjdbcProperties) {

        String schemaName = fjdbcProperties.getSystemName() + "_schema";
        String connName = fjdbcProperties.getSystemName() + "_CONNECTION";
        String schemaDrop = "drop virtual schema if exists " + schemaName + " cascade";

        String connectionUrl = fjdbcProperties.getUrl() + ";validateservercertificate=0";

        String connReg = "CREATE OR REPLACE CONNECTION " + connName + "\n" +
                "TO '" + connectionUrl + "'\n" +
                "USER '" + fjdbcProperties.getUser() + "'\n" +
                "IDENTIFIED BY '" + fjdbcProperties.getPassword() + "';";

        String exaConnUrl = "EXA_" + connName;
        String exaConnReg = "CREATE OR REPLACE CONNECTION " + exaConnUrl + "\n" +
                "TO '" + fjdbcProperties.getHostName() + ":" + fjdbcProperties.getPort() + "'\n" +
                "USER '" + fjdbcProperties.getUser() + "'\n" +
                "IDENTIFIED BY '" + fjdbcProperties.getPassword() + "'";

        String schemaReg = "CREATE VIRTUAL SCHEMA " + schemaName + " \n" +
                "USING ADAPTER.JDBC_ADAPTER WITH\n" +
                "    CONNECTION_NAME = '" + connName + "'\n" +
                "    SCHEMA_NAME     = '" + fjdbcProperties.getDatabaseName() + "'\n" +
                "    IMPORT_FROM_EXA = 'true'\n" +
                "    EXA_CONNECTION  = '" + exaConnUrl + "';";
        try {

            //System.out.println(schemaReg);

            Connection connection = DriverManager.getConnection(interactor.getJDBCProperties().getUrl(),
                    interactor.getJDBCProperties().getUser(), interactor.getJDBCProperties().getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(schemaDrop);
            stmt.executeUpdate(connReg);
            stmt.executeUpdate(exaConnReg);
            stmt.executeUpdate(schemaReg);

            stmt.close();
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void registerForeignTable(Interactor interactor, JDBCProperties jdbcProperties, String tableName, String foreignSystemName) {

    }
}
