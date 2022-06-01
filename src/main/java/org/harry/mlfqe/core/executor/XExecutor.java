package org.harry.mlfqe.core.executor;

import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.optimizer.Join;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.examples.traversal.xda.XNode;

import java.util.HashMap;

public class XExecutor {

    SystemCatalog sc;

    public XExecutor(SystemCatalog sc) {
        this.sc = sc;
    }

    public void execute(XNode plan, String sqlQuery, boolean emulateLocal, Interactor localInteractor) throws ClassNotFoundException {

        //System.out.println("Operator tree:");
        //Utils.printJoinTree(plan);
        prepare(plan, emulateLocal, localInteractor);
        Join finalJoin = ((Join) plan.data);

        HashMap<String, String> aliasMap = Utils.getAliasMap(sqlQuery);
        for (String table : aliasMap.keySet()) {
            //System.out.println("Remove " + aliasMap.get(table));
            sqlQuery = sqlQuery.replace(aliasMap.get(table) + ".", "");
        }
        String finalQuery = sqlQuery.replaceAll("(  from)[^&]*(where)", "$1 " + finalJoin.getJoinName() + " $2");

        finalQuery = finalQuery.replaceAll("(?<=where)[\\S\\s]*(?=group)", " 1=1 ");

        if (emulateLocal)
            localInteractor.executeQueryAndPrintResult(finalQuery);
        else
            finalJoin.onDbms.interactor.executeQueryAndPrintResult(finalQuery);

    }

    public void prepare(XNode node, boolean emulateLocal, Interactor localInteractor) {

        if (node != null) {

            prepare(node.left, emulateLocal, localInteractor);
            prepare(node.right, emulateLocal, localInteractor);
            if (node.data instanceof Join) {

                Join join = ((Join) node.data);
                //System.out.println(join.onDbms.name + ": PREPARING JOIN BETWEEN : " + join.lhs + " AND " + join.rhs);
                //get local and foreign systems/tables

                Interactor interactor = join.onDbms.interactor;
                //local join => only register join view
                if (interactor.equals(join.lhs.dbms.interactor) && join.onDbms.interactor.equals(join.rhs.dbms.interactor)) {
                    if (emulateLocal)
                        localInteractor.registerJoinView(join.getJoinName(), join.lhs.shortName, join.rhs.shortName, join.getJoinPredStr());
                    else
                        interactor.registerJoinView(join.getJoinName(), join.lhs.shortName, join.rhs.shortName, join.getJoinPredStr());
                }
                //foreign join => define local and foreign relations & check join type
                else {
                    Relation localRel;
                    Relation foreignRel;
                    if (join.onDbms.equals(join.lhs.dbms)) {
                        localRel = join.lhs;
                        foreignRel = join.rhs;
                    } else {
                        localRel = join.rhs;
                        foreignRel = join.lhs;
                    }


                    if(!emulateLocal)
                        interactor.registerForeignTable(sc, foreignRel.dbms.name, foreignRel.shortName);

                    String foreignTableName = foreignRel.shortName;
                    if (join.joinType.equals("materialize")) {
                        foreignTableName += "$m";
                        if(emulateLocal)
                            localInteractor.registerLocalMaterializedView(foreignTableName, "SELECT * FROM " + foreignRel.shortName);
                        else
                            interactor.registerLocalMaterializedView(foreignTableName, "SELECT * FROM " + foreignRel.shortName);
                    }
                    if(emulateLocal)
                        localInteractor.registerJoinView(join.getJoinName(), localRel.shortName, foreignTableName, join.getJoinPredStr());
                    else
                        interactor.registerJoinView(join.getJoinName(), localRel.shortName, foreignTableName, join.getJoinPredStr());
                }
            }
        }
    }
}
