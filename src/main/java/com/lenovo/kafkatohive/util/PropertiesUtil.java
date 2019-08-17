package com.lenovo.kafkatohive.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties getProperties(String filePath) {
        Properties prop = new Properties();
        InputStream in = new PropertiesUtil().getClass().getResourceAsStream(filePath);
        try {
            prop.load(in);
        } catch (IOException e) {
            throw new RuntimeException("文件路径异常，FilePath:" + filePath);
        }
        return prop;
    }
}
