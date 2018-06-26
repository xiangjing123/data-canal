package com.tmg.internship.datacanal.datainit;

import com.tmg.commons.mq.model.MQMessageStatus;
import com.tmg.internship.datacanal.escenter.exception.ESException;
import com.tmg.internship.datacanal.producer.common.OrderMessageProducer;
import com.tmg.utils.StringUtils;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * 数据库初始化
 *
 * @author xiangjing
 * @date 2018/5/23
 * @company 天极云智
 */

public class DataBaseInit {

    public static final Logger logger = LoggerFactory.getLogger(DataBaseInit.class);

    private static final String all_tables = "*";

    //驱动
    private static String driverName;

    //数据库连接
    private static String connectionUrl;

    //用户名
    private static String username;

    //密码
    private static String passwod;

    private static Connection connection;

    /**
     * 查询数据库分页
     */
    private static final int pageSize = 100;


    /**
     * 是否初始化
     */
    private Boolean defaultOpen = Boolean.FALSE;

    /**
     * 需要初始化的表
     */
    private String initTables = all_tables;

    /**
     * 排除初始化的表
     */
    private String init_unclude_tables;

    /**
     * 消息发送topic
     */
    private String MQS_CANAL_EVENT_TOPIC;

    /**
     * insert 事件
     */
    private static String MQS_CANAL_EVENT_INSERT_KEY = "INSERT";


    private static ResultSetMetaData metaData = null;

    private static Map<String, Object> map = null;

    private static String schemaName = null;

    private static List<Object> keys;

    private static List<Map<String, Object>> arrayList = null;

    /**
     * 初始化数据连接
     */
    public static void initConnection() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        connection = DriverManager.getConnection(connectionUrl, username, passwod);

    }

    /**
     * 初始化
     */
    public void initData() throws SQLException, ClassNotFoundException {
        if (defaultOpen == Boolean.TRUE) {//如果需要初始化

            initConnection();//初始化数据库连接

            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    logger.info("Start Initialization Mysql data....");
                    Thread.currentThread().setName("InitMysqlData");
                    List<String> tables = null;
                    PreparedStatement ps = null;
                    tables = new ArrayList<>();
                    ResultSet rs = null;
                    if (StringUtils.isEmpty(initTables) || initTables.equals(all_tables)) {//如果未设置或者参数为*则默认使用所有的
                        try {
                            ps = connection.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' and  TABLE_SCHEMA=?");
                            schemaName = connection.getCatalog();
                            ps.setObject(1, schemaName);
                            rs = ps.executeQuery();
                            if (null != rs) {
                                while (rs.next()) {
                                    tables.add(rs.getString(1));
                                }
                            }
                        } catch (SQLException e) {
                            close(connection, ps, rs);
                            throw new ESException("mysql 数据初始化 获取数据失败!", e);
                        }

                    } else {
                        try {
                            schemaName = connection.getCatalog();
                            String[] arrs =initTables.split(",");
                            for(String table:arrs){
                                tables.add(table);
                            }

                        } catch (SQLException e) {
                            close(connection, ps, rs);
                            throw new ESException("mysql 数据初始化 获取数据失败!", e);
                        }
                    }

                    if (null == tables || tables.size() == 0) {
                        throw new ESException("需要导入的数据表格为空，初始化失败!");
                    }

                    uncludeTables(tables);//排除不需要初始化的表格

                    try {
                        for (String table : tables) {
                            initTable(table, connection, ps, rs);
                        }
                    } catch (Exception e) {
                        close(connection, ps, rs);
                        throw new ESException("mysql数据初始化失败", e);
                    }

                }
            });

            thread.start();//开始线程
        }

    }

    /**
     * 排除不需要初始化的表格
     *
     * @param tables
     */
    private void uncludeTables(List<String> tables) {
        if (!StringUtils.isEmpty(this.init_unclude_tables)) {//排除不需要进行初始化的表格
            String regex = null;
            String[] unclouds = init_unclude_tables.split(",");
            Iterator<String> iterator = tables.iterator();
            while (iterator.hasNext()) {
                String table = iterator.next();
                for (String uncloud : unclouds) {
                    regex =uncloud;
                    if(regex.contains("*")){
                        regex=regex.replace("*","[_,\\d,A-Z,a-z]*")+"$";
                    }
                    if (table.matches(regex)) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }
    }

    /**
     * 查询表数据并发送message 到mq
     * @param table
     * @param connection
     * @param ps
     * @param rs
     */

    private void initTable(String table, Connection connection, PreparedStatement ps, ResultSet rs) {
        int pageNo=getPageNo(table,connection,ps,rs);
        for(int i=0;i<pageNo;i++){
            pagingQuery(table,connection,ps,rs,i);
        }
    }

    /**
     * 分页查询数据库
     *
     * @param table
     * @param connection
     * @param ps
     * @param rs
     */
    private int getPageNo(String table, Connection connection, PreparedStatement ps, ResultSet rs) {
        try {
            ps = connection.prepareStatement("select count(*) from " + table);//获取表信息
            rs = ps.executeQuery();
            int count = 0;
            while (rs.next()) {
                count = rs.getInt(1);
            }
            if (count % pageSize == 0) {
                return count / pageSize;
            } else {
                return count / pageSize + 1;
            }

        } catch (SQLException e) {
            throw new ESException(table + ",统计数据条数失败", e);
        }
    }

    /**
     * 分页查询数据库
     *
     * @param table
     * @param connection
     * @param ps
     * @param rs
     * @param page
     */
    private void pagingQuery(String table, Connection connection, PreparedStatement ps, ResultSet rs, int page) {

        try {
            ps = connection.prepareStatement("select * from " + table + " limit ?,?");//获取表信息
            ps.setInt(1, page*pageSize);
            ps.setInt(2, pageSize);
            rs = ps.executeQuery();
            if (null != rs) {
                metaData = rs.getMetaData();
                arrayList = new ArrayList<>();
                while (rs.next()) {
                    map = new HashMap<>();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        map.put(metaData.getColumnName(i), rs.getString(i));

                    }
                    arrayList.add(map);
                }
            } else {
                return;
            }

            DatabaseMetaData dbMeta = connection.getMetaData();
            rs = dbMeta.getPrimaryKeys(null, null, table);
            keys = new ArrayList<>();
            while (rs.next()) {
                keys.add(rs.getObject(4));
            }
            for (Map<String, Object> data : arrayList) {
                sendMsg(data, keys, table);
                try {
                    Thread.currentThread().sleep(getSleepTime());
                } catch (InterruptedException e) {
                    logger.error(table + ",数据表插入数据线程异常");
                    continue;
                }

            }
        } catch (SQLException e) {
            throw new ESException(table + ", 数据表初始化失败,请重新初始化", e);
        }
    }

    /**
     * 获取限流时间
     * @return
     */
    private static long getSleepTime(){
        Integer hour= Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
        if(hour<=8 || hour>=20){
            return 5;
        }else{
            return 50;
        }
    }

    /**
     * 发送消息
     *
     * @param mapList
     */
    private void sendMsg(Map<String, Object> mapList, List<Object> keys, String tableName) {

        String body = buildMQBodyForRowData(mapList, keys);
        //以  库名@表名  为tag，事件类型为Key，发送MQ消息
        MQMessageStatus status = OrderMessageProducer.orderSend(MQS_CANAL_EVENT_TOPIC, schemaName + "@" + tableName, MQS_CANAL_EVENT_INSERT_KEY, body);
        logger.info("XiyouDML event {} send to mqs : {}", MQS_CANAL_EVENT_INSERT_KEY, status.equals(MQMessageStatus.SUCCESS));
    }


    /**
     * 构建insert 事件的数据模板
     *
     * @param map
     * @param keys
     * @return
     */
    private String buildMQBodyForRowData(Map<String, Object> map, List<Object> keys) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"event\":\"").append(MQS_CANAL_EVENT_INSERT_KEY).append("\",");
        sb.append("\"before\":");
        sb.append("{}");
        sb.append(",\"after\":");
        sb.append(JSONObject.fromObject(map).toString());
        sb.append(",\"pk\":");
        sb.append("[");
        for (int i = 0; i < keys.size(); i++) {
            sb.append("\"" + keys.get(i) + "\"");
            if (i != keys.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        sb.append("}");

        return sb.toString();
    }

    /**
     * 关闭资源
     *
     * @param connection
     * @param ps
     * @param rs
     */
    protected void close(Connection connection, PreparedStatement ps, ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
        } catch (SQLException e) {
            logger.error(rs + "资源关闭失败,{}" + e);
        }
        try {
            if (null != ps) {
                ps.close();
            }
        } catch (SQLException e) {
            logger.error(ps + "资源关闭失败,{}" + e);
        }
        try {
            if (null != connection) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error(connection + "资源关闭失败,{}" + e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Boolean getDefaultOpen() {
        return defaultOpen;
    }

    public void setDefaultOpen(Boolean defaultOpen) {
        this.defaultOpen = defaultOpen;
    }

    public String getInitTables() {
        return initTables;
    }

    public void setInitTables(String initTables) {
        this.initTables = initTables;
    }

    public String getInit_unclude_tables() {
        return init_unclude_tables;
    }

    public void setInit_unclude_tables(String init_unclude_tables) {
        this.init_unclude_tables = init_unclude_tables;
    }

    public String getMQS_CANAL_EVENT_TOPIC() {
        return MQS_CANAL_EVENT_TOPIC;
    }

    public void setMQS_CANAL_EVENT_TOPIC(String MQS_CANAL_EVENT_TOPIC) {
        this.MQS_CANAL_EVENT_TOPIC = MQS_CANAL_EVENT_TOPIC;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswod() {
        return passwod;
    }

    public void setPasswod(String passwod) {
        this.passwod = passwod;
    }
}
