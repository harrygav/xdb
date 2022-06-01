package org.harry.mlfqe.examples.traversal;

public class XDAOp {
    public String expression;
    public String annotation;
    public String tpe;

    public XDAOp(String expression, String annotation, String tpe) {
        this.expression = expression;
        this.annotation = annotation;
        this.tpe = tpe;
    }

    @Override
    public String toString() {
        return expression + '@' + annotation;
    }
}
