package edu.brown.lasvegas.toy;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Assorted utility methods.
 */
public final class LVUtil {
    private static Logger LOG = Logger.getLogger(LVUtil.class);

    public static void outputMemory() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long allocatedMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        LOG.info("Memory " + (maxMemory / 1000000L) + " allocated MB, " + (allocatedMemory / 1000000L) + " max MB, "
                        + ((freeMemory + (maxMemory - allocatedMemory)) / 1000000L) + " free MB");
    }

    public static long queryLong(Statement statement, String sql) throws SQLException {
        ResultSet results = query(statement, sql);
        results.next();
        long ret = results.getLong(1);
        results.close();
        return ret;
    }

    public static double queryDouble(Statement statement, String sql) throws SQLException {
        ResultSet results = query(statement, sql);
        results.next();
        double ret = results.getDouble(1);
        results.close();
        return ret;
    }

    public static ArrayList<String> queryStringArray(Statement statement, String sql) throws SQLException {
        ArrayList<String> ret = new ArrayList<String>();
        ResultSet results = query(statement, sql);
        while (results.next()) {
            ret.add(results.getString(1));
        }
        results.close();
        return ret;
    }

    public static ResultSet query(Statement statement, String sql) throws SQLException {
        LOG.info("executing ..:" + sql);
        ResultSet results = statement.executeQuery(sql);
        LOG.info("executed");
        return results;
    }

    public static String getCurrentDbname(Statement statement) throws SQLException {
        ResultSet results = query(statement, "SELECT db_name()");
        results.next();
        String dbname = results.getString(1);
        LOG.info("dbname=" + dbname);
        results.close();
        return dbname;
    }

    public static ArrayList<String> getClusterKeys(Statement statement, String table) throws Exception {
        String sql = " SELECT syscolumns.name FROM syscolumns"
                        + " INNER JOIN sysindexkeys ON (sysindexkeys.id=syscolumns.id AND syscolumns.colorder=sysindexkeys.colid)"
                        + " WHERE sysindexkeys.id=(SELECT id FROM sysobjects WHERE name='" + table + "')" + " AND sysindexkeys.indid=1"
                        + " ORDER BY sysindexkeys.keyno";
        return queryStringArray(statement, sql);
    }

    public static boolean doesExistTable(Statement statement, String table) throws Exception {
        return !queryStringArray(statement, "SELECT id FROM sysobjects WHERE name='" + table + "'").isEmpty();
    }

    public static int update(Statement statement, String sql) throws SQLException {
        LOG.info("executing ..:" + sql);
        int result = statement.executeUpdate(sql);
        LOG.info("executed. " + result + " rows affected");
        return result;
    }

    public static boolean execute(Statement statement, String sql) throws SQLException {
        LOG.info("executing ..:" + sql);
        boolean result = statement.execute(sql);
        LOG.info("executed. result:" + result);
        return result;
    }

    /** convert List<Integer> to int[]. */
    public static int[] asIntArray(List<Integer> list) {
        int length = list.size();
        Integer[] objectArray = list.toArray(new Integer[length]);
        int[] array = new int[length];
        for (int i = 0; i < length; ++i) {
            array[i] = objectArray[i];
        }
        return array;
    }

    public static void dropIfExistsMSSql(Statement statement, String table) throws SQLException {
        LOG.info("dropping ..:" + table);
        statement.execute("IF EXISTS(SELECT name FROM sysobjects where name=N'" + table + "') DROP TABLE " + table);
        LOG.info("dropped.:" + table);
    }

    public static void executeDdlFile(Statement statement, String ddlFile) throws SQLException, IOException {
        LOG.info("Executing " + ddlFile + "...");
        // caution: DTA's default output is UTF-16LE.
        String encoding = "UTF-8"; // assume UTF-8 or UTF-16.
        try {
            FileInputStream in = new FileInputStream(ddlFile);
            if (in.available() >= 2) {
                int bom1 = in.read();
                int bom2 = in.read();
                if ((bom1 == 0xFF && bom2 == 0xFE) || (bom1 == 0xFE && bom2 == 0xFF)) {
                    encoding = "UTF-16";
                }
            }
            in.close();
        } catch (Exception ex) {
            LOG.info("what's the encoding is?", ex);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(ddlFile), encoding));
        String sql = "";
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            line = line.trim();
            if (line.equalsIgnoreCase("go")) {
                execute(statement, sql);
                sql = "";
            } else {
                sql = sql + " " + line;
            }
        }
    }
}
