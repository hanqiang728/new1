package com.lenovo.kafkatohive.util;





import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.sql.*;
import java.util.*;

/**
 * Created by hanjiang2 on 2019/7/30.
 */
public class JDBCUtil {

    private static String driver = null;
    private static String url = null;
    private static String username = null;
    private static String password = null;

    //读取配置文件,初始化数据库连接信息
    static{
        Properties properties = PropertiesUtil.getProperties("/jdbc.properties");
        driver = properties.getProperty("jdbc.driver");
        url = properties.getProperty("jdbc.url");
        username = properties.getProperty("jdbc.username");
        password = properties.getProperty("jdbc.password");
    }

    //获取数据库连接
    private static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            return conn;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

    public static List<String> getAllTopic(){
        Connection connection = getConn();
        if (null == connection) {
            System.out.println("数据库连接异常,无法连接数据库");
            return null;
        }
        List<String> topic_list = new LinkedList<String>();
        String sql = "select topic from kafkatest";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                String topic = resultSet.getString(1);
                topic_list.add(topic);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }finally {
            close(connection, preparedStatement, resultSet);

        }
        return topic_list;
    }

    public static Map<String,String> getTopicTable(String topic){
        Connection connection = getConn();
        if (null == connection) {
            System.out.println("数据库连接异常,无法连接数据库");
            return null;
        }
        String sql = "select * from "+topic;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String,String> map = new HashMap<String,String>();

        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            while(resultSet.next()){
                map.put(resultSet.getString(1),resultSet.getString(2));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }finally {
            close(connection,preparedStatement,resultSet);
        }
        return map;
    }

    public static void close(Connection connection, PreparedStatement preparedStatement,ResultSet resultSet){
        if (null != resultSet) {
            try {
                resultSet.close();
                resultSet = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != preparedStatement) {
            try {
                preparedStatement.close();
                preparedStatement = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != connection) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
//        getTopicTable("kafkademo");
//        JSONObject map = getTopicTable("kafkademo");
//        for(Object s : map){
//            System.out.println(s);
//        }
//        List<String> list = getAllTopic();
//        String result = JSON.toJSONString(list);
//        System.out.println(result);

    }
}
