package org.harry.mlfqe.core;

import java.util.Properties;

public class JDBCProperties {

    private String systemName;
    private String url;
    private String user;
    private String password;
    private String driverName;
    private String driverJar;
    private String odbcDriver;


    public JDBCProperties() {
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getDriverJar() {
        return driverJar;
    }

    public void setDriverJar(String driverJar) {
        this.driverJar = driverJar;
    }

    public String getOdbcDriver() {
        return odbcDriver;
    }

    public void setOdbcDriver(String odbcDriver) {
        this.odbcDriver = odbcDriver;
    }

    public String getDatabaseName() {
        if (this.url.contains("exa")) {
            String[] args = this.url.split("=");
            return args[1];
        }
        String[] args = this.url.split("/");
        return args[args.length - 1];
    }

    public String getHostName() {
        if (this.url.contains("exa")) {
            String[] args = this.url.split(":");
            return args[2];
        }
        String[] args = this.url.split("/");
        return args[args.length - 2].split(":")[0];
    }

    public String getPort() {
        if (this.url.contains("exa")) {
            String[] args = this.url.split(":");
            return args[3].split(";")[0];
        }
        String[] args = this.url.split("/");
        if (args[args.length - 2].split(":").length > 1)
            return args[args.length - 2].split(":")[1];
        else
            return "";
    }

    public String getTargetSystem() {
        String[] args = this.url.split(":");
        return args[1];
    }

    @Override
    public String toString() {
        return "JDBCProperties{" +
                "systemName='" + systemName + '\'' +
                ", url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", driverName='" + driverName + '\'' +
                ", driverJar='" + driverJar + '\'' +
                '}';
    }


}
