package com.lenovo.kafkatohive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import jodd.datetime.JDateTime;

import java.io.*;
import java.util.List;

/**
 * Created by hanjiang2 on 2019/7/30.
 */
public class FileUtil {

    /**
     *  将字符串保存到文件中
     * @param path     保存文件路径
     * @param content  需要保存的内容
     * @throws IOException
     */
    public static void writeStringToLocal(String path, String content) throws IOException {
        BufferedWriter file = new BufferedWriter(new PrintWriter(new FileWriter(path,true)));
        file.write(content);
        file.flush();
        file.close();
    }

    /**
     *  获取文件大小
     * @param path 文件路径
     */
    public static void  getFileSize(String path){
        File file = new File(path);
        if(file.exists() && file.isFile()){
            String fileName = file.getName();
            System.out.println("文件"+fileName+"的大小是："+((double)file.length()/1024));
        }
    }
    //创建主题对应的目录
    public static void makeDir(List<String> topics, String parent){
        JDateTime jdt = new JDateTime();
        String curr_time = jdt.toString("YYYYMMDD");
        File file = null;
        for(String topic : topics){
           file = new File(parent+"\\"+topic+"_"+curr_time);
            file.mkdirs();
        }
    }


    public static void makeDir(String path){
        File file = new File(path);
        boolean flag = false;
        if(file != null){
            flag = file.mkdirs();
        }


//        File parent = file.getParentFile();
//        System.out.println(parent.getAbsolutePath());
//        if (parent != null) {
//            parent.mkdirs();
//        }
    }


    public static void main(String[] args) {
//        getFileSize("C:\\Users\\hanjiang2\\Desktop\\leapIOT添加下载\\任务.txt");
//        makeDir("C:\\Users\\hanjiang2\\Desktop\\a\\b");
//        JDateTime jdt = new JDateTime();
//        System.out.println();;
        String s = "";
        System.out.println(s);
    }
}
