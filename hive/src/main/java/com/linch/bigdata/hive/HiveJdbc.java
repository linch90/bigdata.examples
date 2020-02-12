package com.linch.bigdata.hive;

import java.sql.*;

public class HiveJdbc {

    private final static String hive_url = "jdbc:hive2://node03:10000/db_hive";

    public static void main(String[] args) throws ClassNotFoundException {

        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = null;
        try{
            connection = DriverManager.getConnection(hive_url, "hadoop", "");
            PreparedStatement ps = connection.prepareStatement("select * from score");
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                String s_id = rs.getString("s_id");
                String c_id = rs.getString("c_id");
                int s_score = rs.getInt("s_score");
                String month = rs.getString("month");
                System.out.printf("s_id: %s, c_id: %s, score: %d, month: %s\n ", s_id, c_id, s_score, month);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
