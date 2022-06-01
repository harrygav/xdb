package org.harry.mlfqe.core.optimizer;

import org.javatuples.Pair;

import java.util.*;

public class Relation implements Comparable<Relation> {

    @Override
    public int compareTo(Relation relation) {
        return Double.compare(this.totalCostRows, relation.totalCostRows);
    }

    public String name;
    public String shortName;
    public DBMS dbms;
    public long rowCount;
    public ArrayList<Attribute> schema;
    public ArrayList<Relation> composedOf;
    public Long cost;
    public double moveCost;
    public double joinCostRows;
    public double joinCostCells;
    public double totalCostRows;
    public double totalCostCells;
    public boolean isBaseRelation;

    public Relation() {
    }

    public long getCellCount() {
        return rowCount * schema.size();
    }

    public long getRowCount() {
        return rowCount;
    }

    public double getJoinCostRows() {
        return joinCostRows;
    }

    public double getJoinCostCells() {
        return joinCostCells;
    }

    public double getTotalCostRows() {
        return totalCostRows;
    }

    public double getTotalCostCells() {
        return totalCostCells;
    }

    public double getCost() {
        return cost;
    }

    public Relation(String name, DBMS dbms, long rowCount, boolean isBaseRelation) {
        this.name = "[" + dbms.name + "]" + name;
        this.shortName = name;
        this.dbms = dbms;
        this.rowCount = rowCount;
        this.schema = new ArrayList<>();
        this.composedOf = new ArrayList<>();
        this.cost = 0L;
        this.moveCost = 0;
        this.joinCostRows = 0;
        this.joinCostCells = 0;
        this.totalCostRows = 0;
        this.totalCostCells = 0;

        this.isBaseRelation = isBaseRelation;
    }

    public void updateDbms(DBMS dbms) {
        String oldDbmsName = this.dbms.name;
        this.dbms = dbms;
        this.name = this.name.replaceFirst(oldDbmsName, dbms.name);
    }

    public Relation getComposedRelationByShortName(String shortName) {
        for (Relation r : this.composedOf) {
            if (r.shortName.equals(shortName))
                return r;
        }
        return null;
    }

    public ArrayList<Relation> getComposedRelationsByDepth(int depth) {
        ArrayList<Relation> rels = new ArrayList<>();
        for (Relation r : this.composedOf) {
            if (r.composedOf.size() == depth)
                rels.add(r);
        }

        Collections.reverse(rels);
        return rels;
    }

    public void addCost(Long cost) {
        this.cost += cost;
    }

    public void addAttribute(Attribute attr) {
        this.schema.add(attr);
    }

    public void addAttributes(ArrayList<Attribute> attrs) {
        this.schema.addAll(attrs);
    }

    public void addComposedOf(ArrayList<Relation> rs) {
        this.composedOf.addAll(rs);
    }

    public void addComposedOf(Relation r) {
        this.composedOf.add(r);
    }

    public Relation getNestedRelation(String name) {
        //System.out.println(this.shortName + " composed of: " + this.composedOf);
        //System.out.println("COMPOSED OF:");
        for (Relation rel : this.composedOf) {
            //System.out.println(rel.shortName + ",");
            if (rel.shortName.equals(name)) {
                return rel;
            }
        }

        return null;
    }

    public static String getShortName(String Rname, String Sname) {
        return Rname.replaceAll("\\[[^\\[]*\\]", "").replaceAll("(\\()|(\\))", "").replaceAll(",", "_") +
                "_" +
                Sname.replaceAll("\\[[^\\[]*\\]", "").replaceAll("(\\()|(\\))", "").replaceAll(",", "_");
    }


    public Relation join(Relation s, Join joinsOn, DBMS onDbms) {
        long d = 1;
        for (Pair<Attribute, Attribute> p : joinsOn.predicate) {
            d *= Math.max(p.getValue0().distinctVals, p.getValue1().distinctVals);
            break;
        }

        long newSize = (this.rowCount * s.rowCount) / d;
        Relation rs = new Relation("(" + this.name + "," + s.name + ")", onDbms, newSize, false);
        rs.rowCount = newSize;
        /*if (this.dbms.equals(onDbms))
            rs.shortName = getShortName(this.shortName, s.shortName);
        else
            rs.shortName = getShortName(s.shortName, this.shortName);*/
        rs.shortName = getShortName(this.shortName, s.shortName);

        rs.addAttributes(this.schema);
        rs.addAttributes(s.schema);

        rs.addComposedOf(rs);
        rs.addComposedOf(this.composedOf);
        rs.addComposedOf(s.composedOf);
        //System.out.println(shortName+" composed of"+composedOf);

        if (rs.composedOf.size() > 2) {
            //add cost of subplan
            rs.addCost(this.rowCount);

            //add data movement cost
            /*if (this.dbms.equals(onDbms))
                rs.addCost(s.size);
            else
                rs.addCost(this.size);*/

        }

        double curMoveCost;
        if (this.dbms.equals(onDbms) && s.dbms.equals(onDbms)) {
            curMoveCost = 1;
        } else if (this.dbms.equals(onDbms)) {
            curMoveCost = s.getCellCount() * s.dbms.digestFactor * this.dbms.ingestFactor;
        } else {
            curMoveCost = this.getCellCount() * this.dbms.digestFactor * s.dbms.ingestFactor;
        }


        rs.moveCost = this.moveCost + s.moveCost + curMoveCost;
        rs.joinCostRows = Math.log(this.rowCount + s.rowCount);
        rs.joinCostCells = Math.log(this.getCellCount() + s.getCellCount());
        rs.totalCostRows = this.totalCostRows + s.totalCostRows + Math.log(curMoveCost * onDbms.joinCostFactor * (this.rowCount + s.rowCount));
        rs.totalCostCells = this.totalCostCells + s.totalCostCells + Math.log(curMoveCost * onDbms.joinCostFactor * (this.getCellCount() + s.getCellCount()));

        //System.out.println(rs.name + " is composed of: " + rs.composedOf.toString());
        return rs;
    }

    public Join getJoin(Relation s, ArrayList<Join> joinGraph) {
        Join newJoin = getJoin(this, s, joinGraph);
        newJoin.lhs = this;
        newJoin.rhs = s;
        return newJoin;
    }

    public boolean canJoin(Relation s, ArrayList<Join> joinGraph) {
        return canJoin(this, s, joinGraph);
    }

    public static Join getJoin(Relation r, Relation s, ArrayList<Join> joinGraph) {
        //System.out.println("join graph:" + joinGraph);
        //System.out.println(r.shortName + ":" + r.schema);
        //System.out.println(s.shortName + ":" + s.schema);
        Join newJoin = new Join();
        for (Join j : joinGraph) {
            for (Pair<Attribute, Attribute> pred : j.predicate) {
                if ((r.schema.contains(pred.getValue0()) && s.schema.contains(pred.getValue1())) ||
                        (r.schema.contains(pred.getValue1()) && s.schema.contains(pred.getValue0())))
                    newJoin.addPredicate(pred.getValue0(), pred.getValue1());
            }
        }


        //System.out.println(r.shortName + " and " + s.shortName + " do not join");
        return newJoin;

    }

    public static boolean canJoin(Relation r, Relation s, ArrayList<Join> joinGraph) {
        //System.out.println("join graph:" + joinGraph);
        //System.out.println(this.shortName + ":" + this.schema);
        //System.out.println(s.shortName + ":" + s.schema);
        for (Join j : joinGraph) {
            for (Pair<Attribute, Attribute> pred : j.predicate) {
                if ((r.schema.contains(pred.getValue0()) && s.schema.contains(pred.getValue1())) ||
                        (r.schema.contains(pred.getValue1()) && s.schema.contains(pred.getValue0())))
                    return true;
            }
        }

        //System.out.println(this.shortName + " and " + s.shortName + " do not join");
        return false;
    }


    public boolean hasAttribute(String name) {
        for (Attribute a : this.schema) {
            if (a.name.equals(name))
                return true;
        }
        return false;
    }

    public Attribute getAttribute(String name) {
        for (Attribute a : this.schema) {
            if (a.name.equals(name))
                return a;
        }
        return null;
    }

    public int numBaseRelations() {
        int i = 0;
        for (Relation rel : this.composedOf)
            if (rel.isBaseRelation)
                i++;

        return i;
    }

    @Override
    public String toString() {
        return
                this.shortName + " composed of:" + this.name + "{totalCost:" + this.totalCostRows + ", rows:" + this.rowCount + "}";
    }

    public void printCostGrow() {
        System.out.println("---------------");
        System.out.println(this.name);
        System.out.println("Composed of:");
        Collections.sort(this.composedOf);
        this.composedOf.forEach(System.out::println);
        System.out.println("---------------");
    }
}
