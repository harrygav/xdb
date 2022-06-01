package org.harry.mlfqe.core.executor;

import org.apache.commons.lang3.StringUtils;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Join;
import org.harry.mlfqe.core.optimizer.Relation;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Executor {


    public double totalAnalyzeTime;

    public Executor() {

        this.totalAnalyzeTime = 0;
    }


    public ExecutionQuery prepare(String sqlQuery, Relation initialPlan, boolean analyze, ArrayList<Join> joinGraph, SystemCatalog sc) throws SQLException, ClassNotFoundException {
        try {
            System.out.println("------------------------------------------------------------------------");

            String plan = initialPlan.name;
            System.out.println("Preparing plan: " + plan + "\n");

            while (plan.contains("(")) {
                //System.out.println("Current plan: " + plan + "\n");

                // find the most internal join on given plan
                String lastJoin = StringUtils.substringBefore(StringUtils.substringAfterLast(plan, "("), ")");
                String lastJoinExecutor = getLastExecutor(lastJoin, plan);
                //System.out.println("lastCondition: " + lastJoin);
                //System.out.println("lastExecDb: " + lastJoinExecutor);

                // explore the two sides of the join
                final String executorDb = StringUtils.substringBetween(lastJoinExecutor, "[", "]"); // the join executor

                // left side
                final String leftPart = StringUtils.substringBefore(lastJoin, ","); // left side of the join
                final String leftDb = StringUtils.substringBetween(leftPart, "[", "]");
                final String leftTable = getFinalTableName(executorDb, leftDb, leftPart, analyze); // tableName if executorDb=leftTableDb, otherwise tableName_leftTableDb (Foreign)

                // right side
                final String rightPart = StringUtils.substringAfter(lastJoin, ","); // right side of the join
                final String rightDb = StringUtils.substringBetween(rightPart, "[", "]");
                final String rightTable = getFinalTableName(executorDb, rightDb, rightPart, analyze); // tableName if executorDb=leftTableDb, otherwise tableName_leftTableDb (Foreign)


                Interactor interactor = sc.get(executorDb);
                //System.out.println("Working with interactor " + interactor.getSystemName());

                System.out.println("FINE");
                String joinPred = getJoinPred(leftTable, rightTable, joinGraph, initialPlan);
                System.out.println("STILL FINE");
                String viewName;
                if (leftDb.equals(rightDb)) {
                    viewName = leftTable + "_" + rightTable;
                } else if (executorDb.equals(leftDb)) {
                    System.out.println(executorDb + ": Register foreign table " + rightTable + " from " + rightDb);
                    interactor.registerForeignTable(sc, rightDb, rightTable);
                    viewName = leftTable + "_" + rightTable;
                } else {
                    System.out.println(executorDb + ": Register foreign table " + leftTable + " from " + leftDb);
                    interactor.registerForeignTable(sc, leftDb, leftTable);
                    viewName = rightTable + "_" + leftTable;
                }


                System.out.println(executorDb + ": Create " + viewName + " pred:" + joinPred);
                interactor.registerJoinView(viewName, leftTable, rightTable, joinPred);

                if (executorDb.equals(leftDb))
                    plan = plan.replace(lastJoinExecutor + "(" + lastJoin + ")", "[" + executorDb + "]" + leftTable + "_" + rightTable);
                else
                    plan = plan.replace(lastJoinExecutor + "(" + lastJoin + ")", "[" + executorDb + "]" + rightTable + "_" + leftTable);
                //System.out.println("new plan: " + plan + "\n");

            }

            String execStr = StringUtils.substringBetween(plan, "[", "]");
            Interactor execInteractor = sc.get(execStr);
            String view = StringUtils.substringAfter(plan, "]");


            HashMap<String, String> aliasMap = Utils.getAliasMap(sqlQuery);
            for (String table : aliasMap.keySet()) {
                //System.out.println("Remove " + aliasMap.get(table));
                sqlQuery = sqlQuery.replace(aliasMap.get(table) + ".", "");
            }
            String optimizedQuery = sqlQuery.replaceAll("(  from)[^&]*(where)", "$1 " + view + " $2");
            //optimizedQuery = optimizedQuery.replaceAll("@(.*?)\\.(.*?)@", "$2");
            //optimizedQuery = optimizedQuery.replaceAll("A*N*D* \\w\\.[^\\s]+ = \\w\\.[^\\s]+", "");
            //optimizedQuery = optimizedQuery.replaceAll("WHERE[\\s]+AND", "WHERE");
        /*optimizedQuery = optimizedQuery.replaceAll("(WHERE)(.*?)[^&](\\.)", "$1$2");
        optimizedQuery = optimizedQuery.replaceAll("(=)(.*?)[^&](\\.)", "$1$2");
        optimizedQuery = optimizedQuery.replaceAll("(AND)(.*?)[^&](\\.)", "$1$2");*/
            optimizedQuery = optimizedQuery.replaceAll("(?<=where)[\\S\\s]*(?=group)", " 1=1 ");


            ExecutionQuery eq = new ExecutionQuery(optimizedQuery, execInteractor, this.totalAnalyzeTime);
            System.out.println("------------------------------------------------------------------------");
            return eq;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String getJoinPred(String lhs, String rhs, ArrayList<Join> joinGraph, Relation initialPlan) {

        Relation leftRel = initialPlan.getNestedRelation(lhs);
        Relation rightRel = initialPlan.getNestedRelation(rhs);

        System.out.println("Composed of");
        initialPlan.composedOf.forEach(System.out::println);
        System.out.println("LHS: " + lhs);
        System.out.println("left rel:" + leftRel);
        System.out.println("right rel:" + rightRel);
        String joinPred = "";

        if (Relation.canJoin(leftRel, rightRel, joinGraph)) {
            /*System.out.println("left");
            leftRel.schema.forEach(System.out::println);
            System.out.println("right");
            rightRel.schema.forEach(System.out::println);*/
            Join join = Relation.getJoin(leftRel, rightRel, joinGraph);
            joinPred += join.getJoinPredStr();
        } else {
            System.out.println("INVALID PLAN: " + leftRel.shortName + " and " + rightRel.shortName + " can not join");
        }

        return joinPred;
    }


    private static String getFinalTableName(String executorDb, String joinDb, String joinPart, boolean analyze) throws SQLException {
        final String partTable = StringUtils.substringAfter(joinPart, "]");

        String finalTable = "";
        if (joinDb.equals(executorDb)) { // executor db matches table db
            finalTable = partTable;
        } else { // executor db does not match table db -> create foreign table
            //PostgresUtils.createForeignTable(joinDb, partTable, executorDb, analyze);
            //finalTable = partTable + "_" + joinDb;
            finalTable = partTable;
        }

        return finalTable;
    }

    private static String getLastExecutor(String lastCondition, String plan) {
        String exec = "";
        String[] stages = StringUtils.split(plan, "()");
        for (int i = 0; i < stages.length; i++) {
            if (stages[i].equals(lastCondition)) {
                exec = stages[i - 1];
            }
        }
        if (exec.contains(",")) {
            exec = exec.split(",")[1];
        }
        return exec;
    }

}

