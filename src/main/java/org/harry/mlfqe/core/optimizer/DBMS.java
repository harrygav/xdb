package org.harry.mlfqe.core.optimizer;

import org.harry.mlfqe.core.Interactor;

public class DBMS {

    public String name;
    public double ingestFactor;
    public double digestFactor;
    public double joinCostFactor;
    public Interactor interactor;

    public DBMS(String name, double ingestFactor, double digestFactor, double joinCostFactor, Interactor interactor) {
        this.name = name;
        this.ingestFactor = ingestFactor;
        this.digestFactor = digestFactor;
        this.joinCostFactor = joinCostFactor;
        this.interactor = interactor;
    }

    public void setInteractor(Interactor interactor) {
        this.interactor = interactor;
    }
}
