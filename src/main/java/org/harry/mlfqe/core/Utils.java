package org.harry.mlfqe.core;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.io.FileUtils;
import org.harry.mlfqe.core.optimizer.Attribute;
import org.harry.mlfqe.core.optimizer.Join;
import org.harry.mlfqe.core.optimizer.Relation;
import org.harry.mlfqe.examples.traversal.xda.XNode;
import org.harry.mlfqe.interactors.dummy.DummyInteractor;
import org.harry.mlfqe.interactors.exasol.ExasolInteractor;
import org.harry.mlfqe.interactors.hive.HiveInteractor;
import org.harry.mlfqe.interactors.mariadb.MariaDBInteractor;
import org.harry.mlfqe.interactors.postgres.PostgresInteractor;
import org.javatuples.Pair;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class Utils {

    public static void printResultSet(ResultSet rs) throws SQLException {

        ResultSetMetaData rsmd = rs.getMetaData();

        //TODO: add to presto/trino?
        boolean isEmpty = false;
        try {
            if (!rs.isBeforeFirst())
                isEmpty = true;
        } catch (java.sql.SQLFeatureNotSupportedException e) {


        }

        if (isEmpty) {
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Empty ResultSet");
            System.out.println("------------------------------------------------------------------------");
        } else {
            System.out.println("------------------------------------------------------------------------");
            System.out.println("Query Result:");
            System.out.println("------------------------------------------------------------------------");
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();

                for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {

                    if (i != 1)
                        sb.append("|");
                    sb.append(rs.getString(i));

                }
                System.out.println(sb.toString());
            }
            System.out.println("------------------------------------------------------------------------");
        }

    }

    public static String getCreateTable(JDBCProperties jdbcProperties, String tableName, String localSystemUrl) {


        try {
            Connection connection = DriverManager.getConnection(jdbcProperties.getUrl(),
                    jdbcProperties.getUser(), jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            if (jdbcProperties.getUrl().contains("exa"))
                tableName = "\"" + tableName + "\"";

            String schemaQuery = "SELECT * FROM " + tableName + " where 1=0";
            //System.out.println("Schema Query:" + schemaQuery);
            ResultSet rs = stmt.executeQuery(schemaQuery);

            ResultSetMetaData rsmd = rs.getMetaData();

            rs.next();
            String createStr = "CREATE FOREIGN TABLE " + tableName.replaceAll("\"", "") + "(";
            for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
                String typeName = JDBCType.valueOf(rsmd.getColumnType(i)).getName();

                //dirty fixes
                //if (typeName.equals("DECIMAL"))
                //    typeName = "DOUBLE";
                if (typeName.equals("NUMERIC"))
                    typeName = "DECIMAL";

                if (localSystemUrl.contains("hive")) {
                    if (typeName.equals("DECIMAL"))
                        typeName = "DOUBLE";
                    if (typeName.equals("DATE"))
                        typeName = "STRING";
                }

                if (jdbcProperties.getSystemName().contains("exa") && rsmd.getColumnName(i).toLowerCase(Locale.ROOT).equals("l_extendedprice") || rsmd.getColumnName(i).toLowerCase(Locale.ROOT).equals("l_discount"))
                    typeName = "DECIMAL";

                createStr += rsmd.getColumnName(i) + " " + typeName;

                if (typeName.equals("CHAR") || typeName.equals("VARCHAR")) {
                    int size = 255;
                    if (rsmd.getColumnDisplaySize(i) < 255)
                        size = rsmd.getColumnDisplaySize(i);

                    createStr += "(" + size + ")";
                }

                createStr += ",";

                /*System.out.println("TYPE: " + rsmd.getColumnType(i));
                System.out.println("JDBC TYPE: " + typeName);
                System.out.println("TYPENAME: " + rsmd.getColumnTypeName(i));
                System.out.println("COL NAME: " + rsmd.getColumnName(i));
                System.out.println("SCHEMA NAME: " + rsmd.getSchemaName(i));
                System.out.println("-------------");*/
            }
            createStr = createStr.replaceAll(",$", "");
            createStr += ")";

            stmt.close();
            connection.close();
            return createStr;

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return "";
    }

    public static ArrayList<Attribute> getAttributes(JDBCProperties jdbcProperties, String tableName, boolean getCount) {


        ArrayList<Attribute> attributes = new ArrayList<>();
        try {
            Connection connection = DriverManager.getConnection(jdbcProperties.getUrl(),
                    jdbcProperties.getUser(), jdbcProperties.getPassword());

            Statement stmt = connection.createStatement();
            if (jdbcProperties.getUrl().contains("hive")) {
                stmt.execute("SET hive.execution.engine=mr");
                stmt.execute("SET hive.execution.engine=spark");
            }

            if (jdbcProperties.getUrl().contains("exa"))
                tableName = "\"" + tableName + "\"";
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " where 1=0");
            ResultSetMetaData rsmd = rs.getMetaData();

            rs.next();
            Long distinctCnt = 10L;
            for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {

                String attrName = rsmd.getColumnName(i);
                if (jdbcProperties.getUrl().contains("hive")) {
                    stmt.execute("SET hive.execution.engine=mr");
                    stmt.execute("SET hive.execution.engine=spark");
                }
                if (jdbcProperties.getUrl().contains("exa"))
                    attrName = "\"" + attrName + "\"";
                if (getCount) {
                    String distinctQuery = "SELECT COUNT(DISTINCT " + attrName + ") FROM " + tableName;
                    ResultSet rs1 = stmt.executeQuery(distinctQuery);
                    rs1.next();
                    distinctCnt = rs1.getLong(1);
                }


                Attribute attr = new Attribute(attrName.toLowerCase(Locale.ROOT).replaceAll("\"", ""), distinctCnt);
                //System.out.println("created attribute: " + attr);

                attributes.add(attr);

            }
            //TODO: correct connection handling everywhere, as described in https://stackoverflow.com/questions/2225221/closing-database-connections-in-java
            stmt.close();
            connection.close();


        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return attributes;
    }

    public static Long getTableSize(Interactor interactor, String tableName, boolean getReal) {
        long tableSize = 100L;

        if (interactor.getSystemName().contains("exa"))
            tableName = "\"" + tableName + "\"";
        String cntQuery = "SELECT COUNT(*) FROM " + tableName;
        try {
            if (getReal) {
                try {
                    Pair<Connection, ResultSet> sql = interactor.executeQueryAndReturnRS(cntQuery);
                    ResultSet rs = sql.getValue1();
                    rs.next();
                    tableSize = rs.getLong(1);
                    sql.getValue1().close();
                    sql.getValue0().close();

                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println("Exception on system " + interactor.getSystemName() + " for query: " + cntQuery);
            e.printStackTrace();
        }
        return tableSize;
    }

    public static ArrayList<Join> getJoinGraph(ArrayList<Relation> baseRelations, ArrayList<String> joins) {
        ArrayList<Join> joinGraph = new ArrayList<>();
        for (String jStr : joins) {

            Join j = new Join();

            String[] keys = jStr.split("=");
            String[] lhs = keys[0].split("\\.");
            String[] rhs = keys[1].split("\\.");
            String lhsTable = lhs[0];
            String lhsAttr = lhs[1].replaceAll("\"", "");
            String rhsTable = rhs[0];
            String rhsAttr = rhs[1].replaceAll("\"", "");

            Relation r = getRelByShortName(baseRelations, lhsTable);
            Relation s = getRelByShortName(baseRelations, rhsTable);

            if (r.hasAttribute(lhsAttr) && s.hasAttribute(rhsAttr)) {
                j.addPredicate(r.getAttribute(lhsAttr), s.getAttribute(rhsAttr));
                j.lhs = r;
                j.rhs = s;

            } else if (r.hasAttribute(rhsAttr) && s.hasAttribute(lhsAttr)) {
                j.addPredicate(r.getAttribute(lhsAttr), s.getAttribute(rhsAttr));
                j.lhs = r;
                j.rhs = s;

            } else {
                System.out.println(r.name + " and " + s.name + " do not join!");
            }
            joinGraph.add(j);
        }
        return joinGraph;
    }

    public static Relation getRelByShortName(ArrayList<Relation> relations, String shortName) {
        for (Relation r : relations) {
            if (r.shortName.equals(shortName))
                return r;
        }
        return null;
    }

    public static Properties loadPropsFromFile(String propertiesFile) {

        Properties prop = new Properties();

        try (InputStream input = new FileInputStream(propertiesFile)) {

            prop.load(input);


        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public static SystemCatalog getSystemCatalog(String sysPropLocation, Properties tableDist) throws IOException {
        SystemCatalog sc = new SystemCatalog();
        for (String sysName : tableDist.stringPropertyNames()) {

            String sysPropFile = sysPropLocation + sysName + ".properties";

            Interactor interactor = null;

            Properties sysProp = new Properties();
            sysProp.load(new FileInputStream(Utils.class.getClassLoader().getResource(sysPropFile).getFile()));
            if (sysProp.getProperty("url").contains("postgres")) {
                interactor = new PostgresInteractor(sysName);
                ((PostgresInteractor) interactor).initialize(Utils.class.getClassLoader().getResource(sysPropFile).getFile());
            }
            if (sysProp.getProperty("url").contains("exa")) {
                interactor = new ExasolInteractor(sysName);
                ((ExasolInteractor) interactor).initialize(Utils.class.getClassLoader().getResource(sysPropFile).getFile());
            }
            if (sysProp.getProperty("url").contains("hive")) {
                interactor = new HiveInteractor(sysName);
                ((HiveInteractor) interactor).initialize(Utils.class.getClassLoader().getResource(sysPropFile).getFile());
            }
            if (sysProp.getProperty("url").contains("mariadb")) {
                interactor = new MariaDBInteractor(sysName);
                ((MariaDBInteractor) interactor).initialize(Utils.class.getClassLoader().getResource(sysPropFile).getFile());
            }
            if (sysProp.getProperty("url").contains("hsql")) {
                interactor = new DummyInteractor(sysName);
                ((DummyInteractor) interactor).initialize(Utils.class.getClassLoader().getResource(sysPropFile).getFile());
            }
            sc.add(interactor);

        }
        return sc;
    }

    public static ArrayList<Pair<String, Interactor>> registerLocalViews(SystemCatalog sc, Properties tableDist, String query, String sf) {
        ArrayList<Pair<String, Interactor>> tableAnnotations = new ArrayList<>();

        try {
            HashMap<String, String> aliasMap = getAliasMap(query);

            for (String sysName : tableDist.stringPropertyNames()) {
                String[] tables = tableDist.getProperty(sysName).split(",");
                for (String table : tables) {
                    if (aliasMap.containsKey(table)) {
                        String tableName = sysName + "_sf" + sf + "_" + table;
                        String localView = getLocalView(tableName, aliasMap.get(table), query);
                        tableAnnotations.add(new Pair(aliasMap.get(table), sc.get(sysName)));

                        //System.out.println(sysName + " registers " + aliasMap.get(table) + ": " + localView);
                        sc.get(sysName).registerLocalView(aliasMap.get(table), localView);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableAnnotations;
    }

    public static ArrayList<Pair<String, Interactor>> registerLocalViews(SystemCatalog sc, Properties tableDist, String query, String sf, boolean emulateLocal, Interactor localInteractor) {
        ArrayList<Pair<String, Interactor>> tableAnnotations = new ArrayList<>();

        try {
            HashMap<String, String> aliasMap = getAliasMap(query);

            for (String sysName : tableDist.stringPropertyNames()) {
                String[] tables = tableDist.getProperty(sysName).split(",");
                for (String table : tables) {
                    if (aliasMap.containsKey(table)) {
                        String tableName = localInteractor.getSystemName() + "_sf" + sf + "_" + table;
                        String localView = getLocalView(tableName, aliasMap.get(table), query);
                        tableAnnotations.add(new Pair(aliasMap.get(table), sc.get(sysName)));

                        //System.out.println(sysName + " registers " + aliasMap.get(table) + ": " + localView);
                        localInteractor.registerLocalView(aliasMap.get(table), localView);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableAnnotations;
    }

    //assumes that all tables are available on system sysName
    public static void updateRealCardinalities(SystemCatalog sc, Properties tableDist, String query, String sf,
                                               String sysName, Relation r, ArrayList<Join> joinGraph) {


        Interactor localInteractor = sc.get(sysName);
        try {
            HashMap<String, String> aliasMap = getAliasMap(query);

            for (String origSysName : tableDist.stringPropertyNames()) {
                String[] tables = tableDist.getProperty(origSysName).split(",");
                for (String table : tables) {
                    if (aliasMap.containsKey(table)) {
                        String tableName = sysName + "_sf" + sf + "_" + table;
                        String localView = getLocalView(tableName, aliasMap.get(table), query);

                        //System.out.println(sysName + " registers " + aliasMap.get(table) + ": " + localView);
                        localInteractor.registerLocalView(aliasMap.get(table), localView);
                    }

                }
            }


            ArrayList<String> shortNames = new ArrayList<>(Arrays.asList(r.shortName.split("_")));
            shortNames.remove(shortNames.get(0));

            for (String shortName : shortNames) {

                Relation rightRel = r.getComposedRelationByShortName(shortName);
                String leftRelStr;
                if (shortNames.indexOf(shortName) == shortNames.size() - 1) {
                    leftRelStr = r.shortName.substring(0, r.shortName.lastIndexOf("_"));
                } else
                    leftRelStr = r.shortName.substring(0, r.shortName.indexOf("_" + rightRel.shortName + "_"));


                Relation leftRel = r.getComposedRelationByShortName(leftRelStr);

                Join rsj = leftRel.getJoin(rightRel, joinGraph);

                localInteractor.registerJoinView(rsj.getJoinName(), rsj.lhs.shortName, rsj.rhs.shortName, rsj.getJoinPredStr());

                Relation joinedRel = r.getComposedRelationByShortName(rsj.getJoinName());
                Pair<Connection, ResultSet> conRes = localInteractor.executeQueryAndReturnRS("SELECT COUNT(*) FROM " + joinedRel.shortName);
                conRes.getValue1().next();
                //System.out.println("Old size for " + joinedRel.shortName + ": " + joinedRel.size);
                long newSize = conRes.getValue1().getLong(1);
                System.out.println(joinedRel.shortName + " size: " + newSize);
                joinedRel.rowCount = newSize;
                //System.out.println("Updated size for " + joinedRel.shortName + ": " + joinedRel.size);
                conRes.getValue1().close();
                conRes.getValue0().close();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static HashMap<String, String> getAliasMap(String query) {
        String aliases = subStrBetween(query, "from", "where");
        String[] aliasArr = aliases.split(",");
        HashMap<String, String> aliasMap = new HashMap<>();
        for (String alias : aliasArr) {
            String[] splitted = alias.split(" as ");

            String tableName = sanitize(splitted[0]);
            String shortAlias = sanitize(splitted[1]);

            aliasMap.put(tableName, shortAlias);
        }
        return aliasMap;

    }

    public static HashMap<String, String> getInversedAliasMap(HashMap<String, String> aliasMap) {
        HashMap<String, String> inversedAliasMap = new HashMap<>();
        aliasMap.forEach((key, value) -> inversedAliasMap.put(value, key));
        return inversedAliasMap;
    }

    public static ArrayList<String> getJoinStr(String query) {


        ArrayList<String> joinStr = new ArrayList<>();
        try {
            String selPredStr = subStrBetween(query, "where", "group");
            //System.out.println(selPredStr);
            String[] selPreds = selPredStr.split(" and ");

            for (String selPred : selPreds) {
                String[] selPredOps = selPred.split(" = | < | > | <= | >= | LIKE ");
                String lhs = selPredOps[0];
                String rhs = selPredOps[1];
                if (lhs.contains(".") && rhs.contains(".")) {
                    joinStr.add(sanitize(selPred));

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return joinStr;
    }

    public static String getLocalView(String tableName, String alias, String query) {
        StringBuilder projection = new StringBuilder();
        StringBuilder selection = new StringBuilder();

        String projAttrs = subStrBetween(query, "select", "from");
        String[] projAttrArr = projAttrs.split(" ");
        String delimiter1 = "";
        for (String projAttr : projAttrArr) {

            if (projAttr.contains(".") && projAttr.split("\\.")[0].equals(alias)) {
                String attr = sanitize(projAttr.split("\\.")[1]);
                if (!projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
            }
        }

        String selPredStr = subStrBetween(query, "where", "group");
        String[] selPreds = selPredStr.split(" and ");
        String delimiter2 = "";
        for (String selPred : selPreds) {
            String[] selPredOps = selPred.split(" = | < | > | <= | >= | LIKE ");
            String lhs = selPredOps[0];
            String rhs = selPredOps[1];
            if (!(lhs.contains(".") && rhs.contains("."))) {

                if (lhs.split("\\.")[0].equals(alias) || rhs.split("\\.")[0].equals(alias)) {
                    selection.append(delimiter2).append(sanitize(selPred.replace(alias + ".", "")));
                    delimiter2 = " AND ";
                }


            } else {
                String attr = sanitize(lhs.split("\\.")[1]);
                if (lhs.split("\\.")[0].equals(alias) && !projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
                attr = sanitize(rhs.split("\\.")[1]);
                if (rhs.split("\\.")[0].equals(alias) && !projection.toString().contains(attr)) {
                    projection.append(delimiter1).append(attr);
                    delimiter1 = ",";
                }
            }
        }

        if (selection.length() > 0)

            selection.insert(0, " WHERE ");


        String localView = "SELECT " + projection + " FROM " + tableName + selection.toString().replaceAll("date'", " date '");
        return localView;

    }

    public static String subStrBetween(String str, String open, String close) {
        if (str == null || open == null || close == null) {
            return null;
        }
        int start = str.indexOf(open);
        if (start != -1) {
            int end = str.indexOf(close, start + open.length());
            if (end != -1) {
                return str.substring(start + open.length(), end);
            }
        }
        return null;
    }

    public static String sanitize(String str) {
        if (!str.contains("'"))
            return str.replace(" ", "").replace("\n", "").replace(",", "");
        else
            return str.replace("\n", "").replace(",", "");
    }

    public static Long writeViewToFile(JDBCProperties jdbcProperties, String viewName, String timeStamp) throws IOException, SQLException {

        Connection connection = DriverManager.getConnection(jdbcProperties.getUrl(),
                jdbcProperties.getUser(), jdbcProperties.getPassword());

        String td = System.getProperty("td");
        String sf = System.getProperty("sf");
        String q = System.getProperty("q");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + viewName);
        String dir = "/tmp/mlfqe/" + timeStamp + "/td" + td + "/sf" + sf + "/q" + q + "/";
        Files.createDirectories(Paths.get(dir));
        String filename = dir + viewName + ".csv";

        CSVWriter writer = new CSVWriter(new FileWriter(filename), CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        writer.writeAll(rs, true);
        writer.close();

        File writtenFile = new File(filename);

        return FileUtils.sizeOf(writtenFile);
    }

    public static String getResourceAsString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (InputStreamReader isr = new InputStreamReader(is);
             BufferedReader br = new BufferedReader(isr);) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            is.close();
        }
        return sb.toString();
    }

    public static void printCalcitePlan(String header, RelNode relTree) {
        try {
            StringWriter sw = new StringWriter();

            sw.append(header).append(":").append("\n");

            RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

            relTree.explain(relWriter);

            System.out.println(sw);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<Relation> updateBaseRelStats(ArrayList<Relation> baseRelations, String relCacheLoc, String attrCacheLoc, int sf) {

        Properties relProps = loadPropsFromFile(Utils.class.getClassLoader().getResource(relCacheLoc).getFile());
        Properties attrProps = loadPropsFromFile(Utils.class.getClassLoader().getResource(attrCacheLoc).getFile());

        for (String tbl : relProps.stringPropertyNames()) {
            for (Relation rel : baseRelations) {
                if (rel.shortName.equals(tbl)) {
                    //System.out.println(rel.shortName);
                    //update row counts
                    int sfN = sf;
                    //TODO: temp fix for cached tpch table stats
                    if (rel.shortName.equals("n") || rel.shortName.equals("r") || rel.shortName.equals("n2"))
                        sfN = 1;
                    rel.rowCount = Long.parseLong(relProps.getProperty(rel.shortName)) * sfN;
                    //update attribute distinct values
                    for (Attribute attr : rel.schema) {
                        if (attr.name.contains("nation") || attr.name.contains("region"))
                            sfN = 1;
                        if (attrProps.stringPropertyNames().contains(attr.name))
                            rel.getAttribute(attr.name).distinctVals = Long.parseLong(attrProps.getProperty(attr.name)) * sfN;
                    }
                }
            }
        }
        return baseRelations;

    }

    public static XNode constructOperatorTree(Relation r, ArrayList<Join> joinGraph) {
        ArrayList<String> shortNames = new ArrayList<>(Arrays.asList(r.shortName.split("_")));

        XNode nodePlan = new XNode(r.getComposedRelationByShortName(shortNames.get(0)));
        shortNames.remove(shortNames.get(0));

        for (String shortName : shortNames) {

            Relation rightRel = r.getComposedRelationByShortName(shortName);
            String leftRelStr;
            if (shortNames.indexOf(shortName) == shortNames.size() - 1) {
                leftRelStr = r.shortName.substring(0, r.shortName.lastIndexOf("_"));
            } else
                leftRelStr = r.shortName.substring(0, r.shortName.indexOf("_" + rightRel.shortName + "_"));

            Relation leftRel = r.getComposedRelationByShortName(leftRelStr);

            //System.out.println("Iteration  joining " + leftRelStr + " and " + rightRel);
            XNode<Relation> rightNode = new XNode<>(rightRel);
            Join rsj = leftRel.getJoin(rightRel, joinGraph);
            rsj.onDbms = r.getComposedRelationByShortName(rsj.getJoinName()).dbms;
            XNode<Join> join = new XNode<>(rsj);
            join.left = nodePlan;
            join.right = rightNode;
            nodePlan = join;

        }
        return nodePlan;
    }

    public static void printJoinTree(XNode node) {

        if (node != null) {

            printJoinTree(node.left);
            printJoinTree(node.right);
            if (node.data instanceof Join) {
                System.out.println(node.data);

            }

        }
    }

    public static void writeJoinTreeToFile(XNode node, File f) throws IOException {

        if (node != null) {

            writeJoinTreeToFile(node.left, f);
            writeJoinTreeToFile(node.right, f);
            if (node.data instanceof Join) {
                FileUtils.writeStringToFile(f, node.data.toString() + "\n", StandardCharsets.UTF_8, true);

            }

        }
    }

}
