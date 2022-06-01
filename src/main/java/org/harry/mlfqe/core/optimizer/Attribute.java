package org.harry.mlfqe.core.optimizer;

import java.util.ArrayList;

public class Attribute {

    public String name;
    public Long distinctVals;

    public Attribute(String name, Long distinctVals) {
        this.name = name;
        this.distinctVals = distinctVals;
    }

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", distinctVals=" + distinctVals +
                '}';
    }
}
