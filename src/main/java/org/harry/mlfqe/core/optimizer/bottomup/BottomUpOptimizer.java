package org.harry.mlfqe.core.optimizer.bottomup;

import com.google.common.io.Resources;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.DBMS;
import org.harry.mlfqe.core.optimizer.Join;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.examples.traversal.xda.XNode;
import org.harry.mlfqe.interactors.dummy.DummyInteractor;
import org.javatuples.Pair;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

import static org.harry.mlfqe.core.optimizer.Optimizer.getBaseRelations;

public class BottomUpOptimizer {

    public SystemCatalog sc;
    public XNode nodePlan;
    public ArrayList<Relation>[] partialPlans;

    public BottomUpOptimizer(SystemCatalog sc, int joinSize) {

        this.sc = sc;
        this.partialPlans = new ArrayList[joinSize];
        for (int i = 0; i < joinSize; i++)
            partialPlans[i] = new ArrayList<>();


    }

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {

        String query = "3";
        String td = "0";
        URL url = Resources.getResource("tpchq/q" + query + ".sql");
        String q = Resources.toString(url, StandardCharsets.UTF_8);


        Properties tableDist = Utils.loadPropsFromFile(BottomUpOptimizer.class.getClassLoader().getResource("tpchq/td" + td + ".properties").getFile());

        SystemCatalog sc = Utils.getSystemCatalog("local_dbconfig/", tableDist);

        ((DummyInteractor) sc.get("hsql")).loadTables("tpchq/create_tpch.sql");

        //((DummyInteractor) sc.get("hsql")).executeQueryAndPrintResult("SELECT * FROM hsql_sf1_nation1");
        ArrayList<Pair<String, Interactor>> tableAnnotations = Utils.registerLocalViews(sc, tableDist, q, "1");

        ArrayList<Relation> baseRelations = getBaseRelations(tableAnnotations, false, sc);

        String cacheUrl = "tpchq/stats_cache/q" + query + "/";
        //Utils.updateBaseRelStats(baseRelations, cacheUrl + "relations.properties", cacheUrl + "attributes.properties", 1);

        ArrayList<String> joins = Utils.getJoinStr(q);
        ArrayList<Join> joinGraph = Utils.getJoinGraph(baseRelations, joins);
        BottomUpOptimizer bup = new BottomUpOptimizer(sc, baseRelations.size());
        Relation globalPlan = bup.goptimizeDp(baseRelations, joinGraph);
        System.out.println("Final plan: " + globalPlan.shortName);

        //XNode optimizedGlobalPlan = bup.loptimize(globalPlan, joinGraph);
        //bup.printJoinTree(optimizedGlobalPlan);
        sc.cleanUp();
        //XExecutor xExecutor = new XExecutor(sc);
        //xExecutor.execute(bup.nodePlan, q);


        //baseRelations.forEach(x -> x.schema.forEach(System.out::println));
    }

    public XNode loptimize(Relation globalPlan, ArrayList<Join> joinGraph) {

        //ArrayList<Relation> baseRels = globalPlan.getComposedRelationsByDepth(1);
        System.out.println("Traverse");
        this.traverseAndCost(this.nodePlan);

        return this.nodePlan;
    }

    public Join getJoinWithCost(DBMS onDbms, Join join, String mode) {
        //System.out.println("MODE: " + mode);
        Join retJoin = new Join();
        long cost = 0;
        Relation localRel = null;
        Relation remoteRel = null;

        if (join.lhs.dbms.equals(onDbms)) {
            localRel = join.lhs;
            remoteRel = join.rhs;
        } else if (join.rhs.dbms.equals(onDbms)) {
            localRel = join.rhs;
            remoteRel = join.lhs;
        }
        Interactor interactor = this.sc.get(onDbms.name);

        //import foreign table
        if (!onDbms.equals(remoteRel.dbms)) {
            interactor.registerForeignTable(this.sc, remoteRel.dbms.name, remoteRel.shortName);
            boolean analyze = false;
            if (remoteRel.isBaseRelation)
                analyze = true;
            interactor.updateStatistics(remoteRel.shortName, remoteRel, false);
        }


        //create dummy local table if not base table
        if (localRel.numBaseRelations() > 1) {
            interactor.createDummyTable(localRel.shortName, localRel);
        }

        //create dummy table for materialization and add creation and join to the cost
        if (mode.equals("materialize")) {
            String dummyTableName = remoteRel.shortName + "$m";
            cost += interactor.getQueryCost("CREATE TABLE " + dummyTableName + " AS SELECT * FROM " + remoteRel.shortName);
            interactor.createDummyTable(dummyTableName, remoteRel);
            cost += interactor.getQueryCost("SELECT * FROM " + localRel.shortName + "," + dummyTableName + " WHERE " + join.getJoinPredStr());

        } else if (mode.equals("pipeline")) {
            //interactor.registerJoinView(join.getJoinName(), localRel.shortName, remoteRel.shortName, join.getJoinPredStr());
            cost += interactor.getQueryCost(join.getSQL());
            /*try {
                interactor.executeQueryAndPrintResult("DROP VIEW " + join.getJoinName());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }*/

        }

        retJoin.onDbms = onDbms;
        retJoin.joinType = mode;
        long pFactor = 1;
        if (remoteRel.rowCount > localRel.rowCount)
            pFactor = (remoteRel.rowCount / localRel.rowCount) * 10;

       /* if (System.getProperties().containsKey("sf") && Integer.parseInt(System.getProperty("sf")) > 1 && mode.equals("pipeline"))
            pFactor *= Integer.parseInt(System.getProperty("sf"));
        else if (mode.equals("pipeline"))
            pFactor *= 10;*/

        /*if (remoteRel.size > localRel.size)
            pFactor = remoteRel.size / localRel.size;
        else
            pFactor = localRel.size / remoteRel.size;*/

        retJoin.localCost = cost * Math.log(10 + pFactor);
        retJoin.lhs = join.lhs;
        retJoin.rhs = join.rhs;
        retJoin.addPredicates(join.predicate);

        //System.out.println("Finished costing join");
        return retJoin;
    }

    public Relation goptimizeDp(ArrayList<Relation> baseRelations, ArrayList<Join> joinGraph) {
        return goptimizeDp(baseRelations, joinGraph, 0);
    }

    public Relation goptimizeDp(ArrayList<Relation> baseRelations, ArrayList<Join> joinGraph, int getPlanAt) {

        Comparator<Relation> comparator = Comparator.comparing(Relation::getRowCount);
        if (System.getProperties().containsKey("costing")) {
            if (System.getProperty("costing").equals("totalrows"))
                comparator = Comparator.comparing(Relation::getTotalCostRows);
            if (System.getProperty("costing").equals("totalcells"))
                comparator = Comparator.comparing(Relation::getTotalCostCells);
            if (System.getProperty("costing").equals("joinrows"))
                comparator = Comparator.comparing(Relation::getJoinCostRows);
            if (System.getProperty("costing").equals("joincells"))
                comparator = Comparator.comparing(Relation::getJoinCostCells);
            if (System.getProperty("costing").equals("rowcount"))
                comparator = Comparator.comparing(Relation::getRowCount);
        }

        for (Relation r : baseRelations) {
            this.partialPlans[0].add(r);
        }

        for (int i = 1; i < baseRelations.size(); i++) {
            for (Relation partialPlan : this.partialPlans[i - 1]) {
                for (Relation s : baseRelations) {
                    //System.out.println(partialPlan);
                    if (!partialPlan.equals(s) && !partialPlan.composedOf.contains(s) && partialPlan.canJoin(s, joinGraph)) {
                        //System.out.println("Can join with " + s);
                        Join rsj = partialPlan.getJoin(s, joinGraph);
                        Relation newPartialPlan = partialPlan.join(s, rsj, partialPlan.dbms);
                        this.partialPlans[i].add(newPartialPlan);

                    }

                }
            }
            System.out.println("plans at level: " + i);
            for (Relation r : this.partialPlans[i])
                System.out.println(r);
            //keep best plan
            this.partialPlans[i].sort(comparator);
            Relation bestPlan = this.partialPlans[i].get(0);
            this.partialPlans[i] = new ArrayList<Relation>();
            this.partialPlans[i].add(bestPlan);
        }


        this.partialPlans[baseRelations.size() - 1].sort(comparator);

        //System.out.println("Partial plans");
        //this.partialPlans[baseRelations.size() - 1].forEach(x -> System.out.println(x));


        //TODO: get all plans

        if (getPlanAt > this.partialPlans.length - 1)
            getPlanAt = 0;

        Relation finalPlan = this.partialPlans[baseRelations.size() - 1].get(getPlanAt);
        this.nodePlan = Utils.constructOperatorTree(finalPlan, joinGraph);

        if (getPlanAt > this.partialPlans.length - 1)
            return null;
        else
            return finalPlan;
    }

    public Relation goptimizeGreedy(ArrayList<Relation> baseRelations, ArrayList<Join> joinGraph) {
        Relation currentPlan = baseRelations.get(0);
        for (Relation r : baseRelations) {
            if (r.getRowCount() < currentPlan.getRowCount())
                currentPlan = r;
        }
        this.nodePlan = new XNode<>(currentPlan);
        while (!baseRelations.isEmpty()) {
            Relation cheapestNext = getCheapestNext(currentPlan, baseRelations, joinGraph);


            Join rsj = currentPlan.getJoin(cheapestNext, joinGraph);
            currentPlan = currentPlan.join(cheapestNext, rsj, currentPlan.dbms);

            XNode<Relation> rightRel = new XNode<>(rsj.rhs);
            XNode<Join> join = new XNode<>(rsj);
            join.left = this.nodePlan;
            join.right = rightRel;
            this.nodePlan = join;

            //System.out.println("Current plan:" + currentPlan.shortName);
            baseRelations.removeAll(currentPlan.composedOf);
            //System.out.println("Remaining: " + baseRelations.stream().map(x -> x.shortName).collect(Collectors.joining(",")));
        }


        return currentPlan;
    }

    public Relation getCheapestNext(Relation partialPlan, ArrayList<Relation> baseRelations, ArrayList<Join> joinGraph) {

        Relation cheapest = new Relation("max", baseRelations.get(0).dbms, Integer.MAX_VALUE, false);

        for (Relation s : baseRelations) {
            if (partialPlan.canJoin(s, joinGraph)) {

                //System.out.println(current.shortName + " joins " + s.shortName);
                Join rsj = partialPlan.getJoin(s, joinGraph);

                Relation rs = partialPlan.join(s, rsj, partialPlan.dbms);

                if (rs.rowCount < cheapest.rowCount) {
                    cheapest = s;
                }

                /*if (rs.size < cheapest.size) {
                    //System.out.println("discarding " + cheapest.shortName);
                    rsj.rhs = s;
                    XNode<Relation> rightRel = new XNode<>(s);
                    XNode<Join> join = new XNode<>(rsj);
                    join.left = this.nodePlan;
                    join.right = rightRel;
                    this.nodePlan = join;

                    cheapest = rs;
                }*/


            }
        }

        return cheapest;
    }

    public void traverseAndCost(XNode node) {

        if (node != null) {
            traverseAndCost(node.left);
            traverseAndCost(node.right);
            if (node.data instanceof Join) {
                Join j = (Join) node.data;
                System.out.println("--------------------------------------------------");
                System.out.println("COSTING JOIN BETWEEN : " + j.lhs + " AND " + j.rhs);

                //correct left relation's dbms
                System.out.println("Before correction LHS: " + j.lhs.dbms.name + " RHS: " + j.rhs.dbms.name);
                if (node.left != null && node.left.data instanceof Join) {
                    //System.out.println("Need to correct. Relation " + (((Join) node.left.data).lhs) + " but join on " + ((Join) node.left.data).onDbms.name);
                    j.lhs.updateDbms(((Join) node.left.data).onDbms);
                    //System.out.println("After correction LHS: " + j.lhs.dbms.name + " RHS: " + j.rhs.dbms.name);
                }


                ArrayList<Join> costs = new ArrayList<>();

                if (!j.lhs.dbms.equals(j.rhs.dbms)) {
                    if (!System.getProperties().containsKey("mode") ||
                            (System.getProperty("mode").equals("pipe") || System.getProperty("mode").equals("hybrid"))) {
                        Join jlp = getJoinWithCost(j.lhs.dbms, j, "pipeline");
                        System.out.println(jlp);
                        costs.add(jlp);

                        Join jrp = getJoinWithCost(j.rhs.dbms, j, "pipeline");
                        System.out.println(jrp);
                        costs.add(jrp);
                    }
                    if (!System.getProperties().containsKey("mode") ||
                            (System.getProperty("mode").equals("mat") || System.getProperty("mode").equals("hybrid"))) {
                        Join jlm = getJoinWithCost(j.lhs.dbms, j, "materialize");
                        System.out.println(jlm);
                        costs.add(jlm);
                        Join jrm = getJoinWithCost(j.rhs.dbms, j, "materialize");
                        System.out.println(jrm);
                        costs.add(jrm);
                    }
                } else {
                    System.out.println("SAME DBMS: COSTING ONLY PIPELINING");
                    Join jlp = getJoinWithCost(j.lhs.dbms, j, "pipeline");
                    costs.add(jlp);
                }

                Collections.sort(costs);
                System.out.println("Decided: " + costs.get(0));
                node.data = costs.get(0);
                node.options = costs;

            }

        }
    }


}





