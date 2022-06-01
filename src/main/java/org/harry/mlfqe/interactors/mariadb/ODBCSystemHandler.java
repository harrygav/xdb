package org.harry.mlfqe.interactors.mariadb;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public class ODBCSystemHandler implements ForeignSystemHandler {
    public ODBCSystemHandler() {
    }

    @Override
    public void createExtension(JDBCProperties jdbcProperties) {
        String fdw = "INSTALL SONAME 'ha_connect';";
        try {
            Connection connection = DriverManager.getConnection(jdbcProperties.getUrl(),
                    jdbcProperties.getUser(), jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            stmt.execute(fdw);
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    @Override
    public void registerForeignSystem(Interactor interactor, JDBCProperties fjdbcProperties) {

    }

    @Override
    public void registerForeignTable(Interactor interactor, JDBCProperties fjdbcProperties, String tableName, String foreignSystemName) {

        String tblDrop = "DROP TABLE IF EXISTS " + tableName + " CASCADE";

        String createTable =
                //"create table " + tableName + " " +
                Utils.getCreateTable(fjdbcProperties, tableName, interactor.getJDBCProperties().getUrl()).replaceAll("FOREIGN", "") + " " +
                        "engine=connect " +
                        "table_type=ODBC " +
                        "block_size=250000 " +
                        "tabname='" + tableName + "' " +
                        "Connection='Driver=" + fjdbcProperties.getOdbcDriver() + ";" +
                        "Server=" + fjdbcProperties.getHostName() + ";" +
                        "Port=" + fjdbcProperties.getPort() + ";" +
                        "Database=" + fjdbcProperties.getDatabaseName() + ";" +
                        "Uid=" + fjdbcProperties.getUser() + ";" +
                        "Pwd=" + fjdbcProperties.getPassword() + ";'" +
                        ";";

        //TODO: same as in postgres; odbc-config
        if (fjdbcProperties.getTargetSystem().toLowerCase(Locale.ROOT).equals("hive2")) {
            createTable = "create table " + tableName + " " +
                    "engine=connect " +
                    "table_type=ODBC " +
                    "block_size=250000 " +
                    "tabname='" + fjdbcProperties.getDatabaseName() + "." + tableName + "' " +
                    "Connection='Driver=" + fjdbcProperties.getOdbcDriver() + ";" +
                    "Host=" + fjdbcProperties.getHostName() + ";" +
                    "Port=" + fjdbcProperties.getPort() + ";" +
                    "Uid=" + fjdbcProperties.getUser() + ";" +
                    "Pwd=" + fjdbcProperties.getPassword() + ";" +
                    "HiveServerType=2;" +
                    "ServiceDiscoveryMode=0';" +
                    ";";
        }

        try {
            //System.out.println(createTable);
            //Class.forName(this.jdbcProperties.getDriverName());

            Connection connection = DriverManager.getConnection(interactor.getJDBCProperties().getUrl(),
                    interactor.getJDBCProperties().getUser(), interactor.getJDBCProperties().getPassword());

            Statement stmt = connection.createStatement();
            stmt.execute(tblDrop);
            stmt.execute(createTable);
            //stmt.execute(tblReg);

            connection.close();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
