package org.harry.mlfqe.examples.traversal;

import java.util.Stack;

public class PostOrder {
    public Stack<XDAOp> ops;

    public PostOrder() {
        this.ops = new Stack<>();
    }

    public void traverse(Node<XDAOp> node, String currentAnnotation) {
        //System.out.println("Starting traversal");

        if (node.getChildren().size() > 0)
            traverse(node.getChildren().get(0), node.getData().annotation);


        if (node.getChildren().size() > 1)
            traverse(node.getChildren().get(1), node.getData().annotation);

        //System.out.println(node.getData());

        ops.add(node.getData());

        if (!currentAnnotation.equals(node.getData().annotation)) {
            System.out.println("Annotation change!");
            createStatement(ops);
            createForeignTable(currentAnnotation, node.getData().annotation, node.getData() + "_TBL");
            ops.clear();
            ops.add(node.getData());
        }


        currentAnnotation = node.getData().annotation;
        //System.out.println(currentAnnotation);

    }

    public void createStatement(Stack ops) {
        System.out.println("REGISTER LOCAL VIEW:" + ops.peek()+"_TBL" + ops);

    }

    public void createForeignTable(String onSystem, String fromSystem, String table) {
        System.out.println("CREATE FOREIGN TABLE ON " + onSystem + " FROM " + fromSystem + " TABLE " + table);
    }
}
