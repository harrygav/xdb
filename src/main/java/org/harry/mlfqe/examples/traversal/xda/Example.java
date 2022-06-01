package org.harry.mlfqe.examples.traversal.xda;

public class Example {

    public static void main (String[] args)
    {
        XNode<String> scanr = new XNode<>("R");
        XNode<String> scans = new XNode<>("S");
        XNode<String> join = new XNode<>("join1");
        join.left=scanr;
        join.right=scans;
        XNode<String> scant = new XNode<>("T");
        XNode<String> join2 = new XNode<>("join2");
        join2.left=join;
        join2.right=scant;

        traverse(join2);

    }

    public static void traverse(XNode node){

        if(node!=null)
        {
            traverse(node.left);
            traverse(node.right);
            System.out.println(node.data);
        }
    }
}
