package org.harry.mlfqe.interactors.hive;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.JDBCProperties;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.interactors.postgres.ForeignSystemHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public class JDBCSystemHandler implements ForeignSystemHandler {
    public JDBCSystemHandler() {
    }

    @Override
    public void createExtension(JDBCProperties jdbcProperties) {

    }

    @Override
    public void registerForeignSystem(Interactor interactor, JDBCProperties fjdbcProperties) {

    }

    @Override
    public void registerForeignTable(Interactor interactor, JDBCProperties fjdbcProperties, String tableName, String foreignSystemName) {


        String tblDrop = "DROP TABLE IF EXISTS " + tableName;

        String dbType = "POSTGRES";
        if (fjdbcProperties.getTargetSystem().toLowerCase(Locale.ROOT).contains("maria"))
            dbType = "MYSQL";

        String createTable = Utils.getCreateTable(fjdbcProperties, tableName, interactor.getJDBCProperties().getUrl()).replaceAll("FOREIGN", "EXTERNAL") + "\n" +
                "STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'\n" +
                "TBLPROPERTIES (\n" +
                "\"hive.sql.database.type\" = \"" + dbType + "\",\n" +
                "\"hive.sql.jdbc.url\" = \"" + fjdbcProperties.getUrl() + "\",\n" +
                "\"hive.sql.dbcp.username\" = \"" + fjdbcProperties.getUser() + "\",\n" +
                "\"hive.sql.dbcp.password\" = \"" + fjdbcProperties.getPassword() + "\",\n" +
                "\"hive.sql.jdbc.driver\" = \"" + fjdbcProperties.getDriverName() + "\",\n" +
                "\"hive.sql.query\" = \"SELECT * FROM " + tableName + "\",\n" +
                "\"hive.sql.jdbc.fetch.size\" = \"200000\",\n" +
                "\"hive.sql.dbcp.maxActive\" = \"1\"\n" +
                ")";

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
