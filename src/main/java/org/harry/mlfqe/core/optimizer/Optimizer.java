package org.harry.mlfqe.core.optimizer;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.javatuples.Pair;

import java.sql.SQLException;
import java.util.*;

public class Optimizer {

    public static void main(String[] args) throws SQLException {

        DBMS db1 = new DBMS("db1", 1, 1, 1, null);
        DBMS db2 = new DBMS("db2", 2, 2, 1.5, null);
        DBMS db3 = new DBMS("db3", 1, 1, 2, null);
        DBMS db4 = new DBMS("db4", 2, 2, 2.5, null);

        Relation r = new Relation("r", db1, 100000, true);
        r.addComposedOf(r);
        Attribute a = new Attribute("a", 50000L);
        r.addAttribute(a);
        Attribute b = new Attribute("b", 50000L);
        r.addAttribute(b);

        Relation s = new Relation("s", db2, 200, true);
        s.addComposedOf(s);
        Attribute b1 = new Attribute("b", 200L);
        s.addAttribute(b1);
        Attribute c = new Attribute("c", 300L);
        s.addAttribute(c);

        Relation t = new Relation("t", db3, 10000, true);
        t.addComposedOf(t);
        Attribute c1 = new Attribute("c", 20L);
        t.addAttribute(c1);
        Attribute d = new Attribute("d", 50L);
        t.addAttribute(d);

        Relation u = new Relation("u", db4, 1000, true);
        u.addComposedOf(u);
        Attribute a1 = new Attribute("a", 50L);
        u.addAttribute(a1);
        Attribute d1 = new Attribute("d", 1000L);
        u.addAttribute(d1);

        Join j1 = new Join();
        j1.addPredicate(a, a1);
        Join j2 = new Join();
        j2.addPredicate(b, b1);
        Join j3 = new Join();
        j3.addPredicate(d, d1);
        Join j4 = new Join();
        j4.addPredicate(c, c1);

        ArrayList<Join> joinGraph = new ArrayList<>();
        joinGraph.add(j1);
        joinGraph.add(j2);
        joinGraph.add(j3);
        joinGraph.add(j4);


        ArrayList<Relation> baseRelations = new ArrayList<>();
        baseRelations.add(r);
        baseRelations.add(s);
        baseRelations.add(t);
        baseRelations.add(u);

        ArrayList<Relation> finalPlans = optimize(baseRelations, joinGraph);


        System.out.println("Found " + finalPlans.size() + " plans");

        Collections.sort(finalPlans);
        //finalPlans.forEach(System.out::println);
        /*System.out.println("-----------------");
        System.out.println("Cheapest simpleCost-wise:");
        System.out.println(cheapestCostPlan(finalPlans, "simpleCost"));
        System.out.println("Cheapest moveCost-wise:");
        System.out.println(cheapestCostPlan(finalPlans, "moveCost"));
        System.out.println("Cheapest joinCost-wise:");
        System.out.println(cheapestCostPlan(finalPlans, "joinCost"));*/
        //Relation cheapestMovePlan = cheapestCostPlan(finalPlans, "moveCost");
        //Executor executor = new Executor();
        //executor.prepare("", cheapestMovePlan.name, new HashMap<>(), false);

    }

    public static ArrayList<Relation> optimize(ArrayList<Relation> baseRelations, ArrayList<Join> joinGraph) {
        Stack<Relation> subplans = new Stack<>();
        Collections.reverse(baseRelations);
        subplans.addAll(baseRelations);

        ArrayList<Relation> finalPlans = new ArrayList<>();
        ArrayList<String> memo = new ArrayList<>();

        int i = 0;
        while (!subplans.isEmpty()) {

            /*i++;
            if (i > 10)
                break;*/
            Relation rel = subplans.pop();
            for (Relation baseRel : baseRelations) {
                if (!rel.composedOf.contains(baseRel) && rel.canJoin(baseRel, joinGraph)) {
                    Join joinsOn = rel.getJoin(baseRel, joinGraph);
                    String currentCombo = Relation.getShortName(rel.name, baseRel.name);
                    String currentCombo2 = Relation.getShortName(baseRel.name, rel.name);
                    //System.out.println("CURRENT COMBO: " + currentCombo);
                    if (joinsOn.predicate.size() > 0 && !memo.contains(currentCombo)) {
                        //System.out.println(currentCombo);

                        //System.out.println(memo);
                        Relation moveLeftJoin = rel.join(baseRel, joinsOn, rel.dbms);
                        Relation moveRightJoin = rel.join(baseRel, joinsOn, baseRel.dbms);
                        memo.add(currentCombo);
                        //memo.add(currentCombo2);
                        if (moveLeftJoin.numBaseRelations() == baseRelations.size())
                            finalPlans.add(moveLeftJoin);
                        else
                            subplans.push(moveLeftJoin);
                        if (moveRightJoin.numBaseRelations() == baseRelations.size())
                            finalPlans.add(moveRightJoin);
                        else
                            subplans.push(moveRightJoin);

                        //System.out.println(subplans.toString());
                        //System.out.println("-----------------");
                    }
                }
            }
        }
        return finalPlans;
    }

    public static Relation cheapestCostPlan(ArrayList<Relation> rels, String metric) {
        double min = Double.MAX_VALUE;
        int i = 0;
        int minKey = 0;
        if (metric.equals("moveCost")) {
            for (Relation r : rels) {
                if (r.moveCost < min) {
                    minKey = i;
                    min = r.moveCost;
                }
                i++;
            }
        /*} else if (metric.equals("joinCost")) {
            for (Relation r : rels) {
                if (r.joinRowCost < min) {
                    minKey = i;
                    min = r.joinRowCost;
                }
                i++;
            }*/
        } else if (metric.equals("simpleCost")) {
            for (Relation r : rels) {
                if (r.cost < min) {
                    minKey = i;
                    min = r.cost;
                }
                i++;
            }
        } else if (metric.equals("totalCost")) {
            for (Relation r : rels) {
                if (r.totalCostRows < min) {
                    minKey = i;
                    min = r.totalCostRows;
                }
                i++;
            }
        }
        return rels.get(minKey);
    }

    public static ArrayList<Relation> getBaseRelations(ArrayList<Pair<String, Interactor>> tableAnnotations, boolean getRealStats, SystemCatalog sc) {
        ArrayList<Relation> baseRelations = new ArrayList<>();
        HashMap<String, DBMS> dbmss = new HashMap<>();

        for (Pair p : tableAnnotations) {

            Interactor interactor = (Interactor) p.getValue1();

            String dbmsName = (interactor.getSystemName());
            if (!dbmss.containsKey(dbmsName)) {
                double joinCostFactor = 1;

                if (dbmsName.contains("exa"))
                    joinCostFactor = 2;
                else if (dbmsName.contains("mdb"))
                    joinCostFactor = 32;
                else if (dbmsName.contains("pg"))
                    joinCostFactor = 8;
                else if (dbmsName.contains("hive"))
                    joinCostFactor = 4;

                DBMS dbms = new DBMS(dbmsName, 1, 1, joinCostFactor, sc.get(dbmsName));
                dbmss.put(dbmsName, dbms);
            }
            String tableName = p.getValue0().toString();
            Relation r = new Relation(tableName, dbmss.get(dbmsName), Utils.getTableSize(interactor, tableName, getRealStats), true);
            //System.out.println("------------------------------------------------------------------------");
            //System.out.println("created relation: " + r);
            ArrayList<Attribute> attrs = Utils.getAttributes(interactor.getJDBCProperties(), r.shortName, getRealStats);
            //System.out.println("------------------------------------------------------------------------");
            r.addAttributes(attrs);
            r.addComposedOf(r);
            baseRelations.add(r);

        }
        return baseRelations;
    }
}
