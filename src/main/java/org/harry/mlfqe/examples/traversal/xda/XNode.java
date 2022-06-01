package org.harry.mlfqe.examples.traversal.xda;

public class XNode<T> {
    public T data = null;
    public T options = null;
    public XNode left, right;

    public XNode(T data) {
        this.data = data;
        left = right = null;
    }
}
