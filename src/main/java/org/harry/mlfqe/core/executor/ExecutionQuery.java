package org.harry.mlfqe.core.executor;

import org.harry.mlfqe.core.Interactor;

public class ExecutionQuery {

    public String optimizedQuery;
    public Interactor interactor;
    public double analyzeTime;

    public ExecutionQuery(String optimizedQuery, Interactor interactor, double analyzeTime) {
        this.optimizedQuery = optimizedQuery;
        this.interactor = interactor;
        this.analyzeTime = analyzeTime;
    }

    public void execute() throws ClassNotFoundException {
        this.interactor.executeQueryAndPrintResult(this.optimizedQuery);
    }

    @Override
    public String toString() {
        return "ExecutionQuery{" +
                "query='" + optimizedQuery + '\'' +
                ", db='" + interactor.getSystemName() + '\'' +
                ", analyzeTime=" + analyzeTime +
                '}';
    }
}