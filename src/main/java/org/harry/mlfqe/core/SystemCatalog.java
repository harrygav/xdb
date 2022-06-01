package org.harry.mlfqe.core;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

public class SystemCatalog {

    HashMap<String, Interactor> catalog;

    public SystemCatalog() {
        this.catalog = new HashMap<>();
    }

    public void add(Interactor interactor) {
        this.catalog.put(interactor.getSystemName(), interactor);
    }

    public HashMap<String, Interactor> getAll() {
        return this.catalog;
    }

    public Interactor get(String systemName) {
        if (this.catalog.containsKey(systemName))
            return this.catalog.get(systemName);

        System.out.println("Interactor with " + systemName + " does not exist");
        return null;
    }

    public void cleanUp() {
        System.out.println("CLEANING UP");
        //TODO: enable
        for (String sys : this.catalog.keySet()) {
            this.catalog.get(sys).cleanUp();
        }
    }

    public long calculateTransferSizes(String unixTime) throws SQLException, IOException {
        String finalView = "";
        int joins = 0;
        //find final view
        for (String sys : this.catalog.keySet()) {
            for (String v : this.catalog.get(sys).getRegisteredJoinViews()) {
                if (StringUtils.countMatches(v, "_") > joins) {
                    finalView = v;
                    joins = StringUtils.countMatches(v, "_");
                }
            }
        }
        System.out.println("FINAL VIEW: " + finalView);

        //export all views other than the final view to CSV files
        //HashMap<String, Long> viewSizes = new HashMap<>();
        long totalSize = 0;
        for (String sys : this.catalog.keySet()) {
            for (String v : this.catalog.get(sys).getRegisteredJoinViews()) {
                if (!v.equals(finalView)) {
                    long viewSize = Utils.writeViewToFile(this.catalog.get(sys).getJDBCProperties(), v, unixTime);
                    //viewSizes.put(v, viewSize);
                    totalSize += viewSize;
                }
            }
        }
        return totalSize;

    }

    @Override
    public String toString() {

        StringBuilder systems = new StringBuilder();
        String delimiter = "";
        for (String name : this.catalog.keySet()) {
            systems.append(delimiter).append(name);
            delimiter = ",";
            //String value = example.get(name).toString();

            //System.out.println(key + " " + value);
        }
        return "System Catalog:" + systems;


    }
}
