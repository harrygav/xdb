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

public class MariaDBSystemHandler implements ForeignSystemHandler {
    public MariaDBSystemHandler() {
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

        String createTable = "create table " + tableName + " " +
                "engine=CONNECT table_type=MYSQL dbname=" + fjdbcProperties.getDatabaseName() + " tabname=" + tableName + " " +
                "connection='mysql://" + fjdbcProperties.getUser() + ":" +
                fjdbcProperties.getPassword() + "@" +
                fjdbcProperties.getHostName() + ":" +
                fjdbcProperties.getPort() + "/" +
                "';";

        //System.out.println(createTable);
        try {
            //System.out.println(createTable);
            //Class.forName(this.jdbcProperties.getDriverName());

            Connection connection = DriverManager.getConnection(interactor.getJDBCProperties().getUrl(),
                    interactor.getJDBCProperties().getUser(), interactor.getJDBCProperties().getPassword());

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(tblDrop);
            stmt.executeUpdate(createTable);
            //stmt.execute(tblReg);

            stmt.close();
            connection.close();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
