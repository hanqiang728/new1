package com.lenovo.kafkatohive.util;

import jodd.datetime.JDateTime;

import java.util.Random;

/**
 * Created by hanjiang2 on 2019/8/2.
 */
public class DateUtil {
    private static JDateTime jdt = new JDateTime();

    //获取当前时间
    public static String getCurrTime(){
        jdt.setCurrentTime();
        return jdt.toString("YYYYMMDD");
    }
    //获取精准时间
    public static String getCurrTimeStamp(){
        jdt.setCurrentTime();
        return jdt.toString("YYYYMMDDhhmmss");
    }
    public static void main(String[] args) {
//        System.out.println(getCurrTime());
        Random ra =new Random();
        int a = ra.nextInt(5);
        System.out.println(a);

    }

}
