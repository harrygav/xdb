package org.mlfqe.benchmarks;

import com.google.common.io.Resources;
import org.apache.commons.io.FileUtils;
import org.harry.mlfqe.core.Interactor;
import org.harry.mlfqe.core.SystemCatalog;
import org.harry.mlfqe.core.Utils;
import org.harry.mlfqe.core.executor.XExecutor;
import org.harry.mlfqe.core.optimizer.DBMS;
import org.harry.mlfqe.core.optimizer.Join;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.core.optimizer.bottomup.BottomUpOptimizer;
import org.harry.mlfqe.examples.traversal.xda.XNode;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import static org.harry.mlfqe.core.optimizer.Optimizer.getBaseRelations;

public class LocGlobOpt {
    @Test
    public void query() throws ClassNotFoundException, IOException, SQLException {

        long start_prep_time = System.nanoTime();
        //get config
        String sf = System.getProperty("sf");
        String query = System.getProperty("q");
        String td = System.getProperty("td");
        String mode = System.getProperty("mode", "hybrid");
        String experimentTimestamp = System.getProperty("et", "");
        String logFile = System.getProperty("lfile", "");
        String suiteTimeStamp = System.getProperty("ts");
        String emulateLocalStr = System.getProperty("emulateLocal", "false");
        String localInteractorStr = System.getProperty("localInteractor", "");

        URL url = Resources.getResource("tpchq/q" + query + ".sql");
        String sqlQuery = Resources.toString(url, StandardCharsets.UTF_8);

        //initialize system catalog & base tables (local views)
        Properties tableDist = Utils.loadPropsFromFile(BottomUpOptimizer.class.getClassLoader().getResource("tpchq/td" + td + ".properties").getFile());
        SystemCatalog sc = Utils.getSystemCatalog("local_dbconfig/", tableDist);
        ArrayList<Pair<String, Interactor>> tableAnnotations = Utils.registerLocalViews(sc, tableDist, sqlQuery, sf);
        ArrayList<Relation> baseRelations = getBaseRelations(tableAnnotations, true, sc);

        //update base relations with cached cardinalities & distinct values
        String cacheUrl = "tpchq/stats_cache/q" + query + "/";
        Utils.updateBaseRelStats(baseRelations, cacheUrl + "relations.properties", cacheUrl + "attributes.properties", Integer.parseInt(sf));

        long end_prep_time = System.nanoTime();

        //get global plan (join order)

        long start_gopt_time = System.nanoTime();
        ArrayList<String> joins = Utils.getJoinStr(sqlQuery);
        ArrayList<Join> joinGraph = Utils.getJoinGraph(baseRelations, joins);
        BottomUpOptimizer bup = new BottomUpOptimizer(sc, baseRelations.size());

        Relation globalPlan = bup.goptimizeDp(baseRelations, joinGraph);
        System.out.println("Optimized global plan: " + globalPlan);

        sc.cleanUp();

        long end_gopt_time = System.nanoTime();
        //System.exit(0);

        //update intermediate relations with real cardinalities
        long start_stats_time = System.nanoTime();
        System.out.println("Finished global optimization. Updating real cardinalities...");
        Utils.updateRealCardinalities(sc, tableDist, sqlQuery, sf, "pg1", globalPlan, joinGraph);
        sc.cleanUp();
        System.out.println("Finished updating real join cardinalities. Starting local join optimization...");
        long end_stats_time = System.nanoTime();

        //get local plan (join placement)
        long start_lopt_time = System.nanoTime();
        Utils.registerLocalViews(sc, tableDist, sqlQuery, sf);
        XNode optimizedGlobalPlan = bup.loptimize(globalPlan, joinGraph);
        System.out.println("--------------------------------------------------");
        System.out.println("Final Plan");
        Utils.printJoinTree(optimizedGlobalPlan);
        sc.cleanUp();
        long end_lopt_time = System.nanoTime();
        System.out.println("Finished local join optimization. Starting execution...");

        //execute plan

        boolean emulateLocal = false;
        Interactor localInteractor = null;
        if (emulateLocalStr.equals("true")) {
            emulateLocal = true;
            localInteractor = sc.get(localInteractorStr);
        }

        long start_exec_time = System.nanoTime();
        if (emulateLocal)
            Utils.registerLocalViews(sc, tableDist, sqlQuery, sf, emulateLocal, localInteractor);
        else
            Utils.registerLocalViews(sc, tableDist, sqlQuery, sf);
        XExecutor xExecutor = new XExecutor(sc);


        xExecutor.execute(bup.nodePlan, sqlQuery, emulateLocal, localInteractor);

        long end_exec_time = System.nanoTime();
        double execTime = (end_exec_time - start_exec_time) / 1e6;
        double loptTime = (end_lopt_time - start_lopt_time) / 1e6;
        double goptTime = (end_gopt_time - start_gopt_time) / 1e6;
        double prepTime = (end_prep_time - start_prep_time) / 1e6;
        double statsTime = (end_stats_time - start_stats_time) / 1e6;


        System.out.println("Executed in " + execTime + "ms");
        System.out.println("Total time: " + (goptTime + loptTime + execTime) + "ms");
        //log stuff
        if (!logFile.equals("")) {
            String planFileName = suiteTimeStamp + "/" + experimentTimestamp + "_" +
                    logFile.replaceAll(".csv", "_td" + td + "_sf" + sf + "_q" + query + "_mode_" + mode + "_plan.txt");
            File planFile = new File(planFileName);
            Utils.writeJoinTreeToFile(optimizedGlobalPlan, planFile);

            File file = new File(logFile);
            //timestamp,plan,preptime,statstime,gopttime,lopttime,exectime
            FileUtils.writeStringToFile(file, experimentTimestamp + ",\"" +
                    planFileName + "\"," +
                    prepTime + "," +
                    statsTime + "," +
                    goptTime + "," +
                    loptTime + "," +
                    execTime +
                    "\n", StandardCharsets.UTF_8, true);
        }

        sc.cleanUp();
    }
}
