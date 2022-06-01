package org.harry.mlfqe.examples.traversal;

import java.util.List;

public class ExampleTraversal {

    public static void main(String[] args) {
        Node<XDAOp> g1 = new Node<XDAOp>(new XDAOp("γ", "HDB", "unary"));
        Node<XDAOp> p1 = new Node<XDAOp>(new XDAOp("π", "HDB", "unary"));
        g1.addChild(p1);
        Node<XDAOp> j1 = new Node<XDAOp>(new XDAOp("⨝", "HDB", "binary"));
        p1.addChild(j1);

        Node<XDAOp> j2 = new Node<XDAOp>(new XDAOp("⨝", "CDB", "binary"));
        j1.addChild(j2);

        Node<XDAOp> j3 = new Node<XDAOp>(new XDAOp("⨝", "VDB", "binary"));
        j2.addChild(j3);

        Node<XDAOp> p2 = new Node<XDAOp>(new XDAOp("π", "VDB", "unary"));
        j3.addChild(p2);
        Node<XDAOp> v = new Node<XDAOp>(new XDAOp("V", "VDB", "tablescan"));
        p2.addChild(v);

        Node<XDAOp> p3 = new Node<XDAOp>(new XDAOp("π", "VDB", "unary"));
        j3.addChild(p3);
        Node<XDAOp> vn = new Node<XDAOp>(new XDAOp("VN", "VDB", "tablescan"));
        p3.addChild(vn);

        Node<XDAOp> p4 = new Node<XDAOp>(new XDAOp("π", "CDB", "unary"));
        j2.addChild(p4);
        Node<XDAOp> s1 = new Node<XDAOp>(new XDAOp("σ", "CDB", "unary"));
        p4.addChild(s1);
        Node<XDAOp> c = new Node<XDAOp>(new XDAOp("C", "CDB", "tablescan"));
        s1.addChild(c);

        Node<XDAOp> p5 = new Node<XDAOp>(new XDAOp("π", "HDB", "unary"));
        j1.addChild(p5);
        Node<XDAOp> m = new Node<XDAOp>(new XDAOp("M", "HDB", "tablescan"));
        p5.addChild(m);

        PostOrder po = new PostOrder();
        po.traverse(g1, "");

        //System.out.println(g1.getChildren());

    }

}
