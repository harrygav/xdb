package org.harry.mlfqe.examples;

import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.interactors.postgres.PostgresInteractor;

import java.io.IOException;
import java.sql.SQLException;

public class PlannerExample {

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        SystemCatalog sc = new SystemCatalog();

        PostgresInteractor pg1 = new PostgresInteractor("PG1");
        pg1.initialize(PlannerExample.class.getClassLoader().getResource("local_dbconfig/postgres1.properties").getFile());
        sc.add(pg1);

        PostgresInteractor pg2 = new PostgresInteractor("PG2");
        pg2.initialize(PlannerExample.class.getClassLoader().getResource("local_dbconfig/postgres2.properties").getFile());
        sc.add(pg2);


        //System.out.println(Utils.getCreateTable(pg1, "PG1", "aka_name"));
        pg1.registerForeignTable(sc, "PG2", "orders");

        //pg1.registerJoinView("aka_title__title", "aka_name", "orders", "aka_name.id=orders.o_orderkey");


        pg1.executeQueryAndPrintResult("SELECT * FROM orders LIMIT 2");

    }
}
