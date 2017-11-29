package luna.util;

/**
 *
 * Copyright: Copyright (c) 2017 XueErSi
 *
 * @version v1.0.0
 * @author GaoXing Chen
 *
 * Modification History:
 * Date         Author          Version			Description
 *---------------------------------------------------------*
 * 2017年8月21日     GaoXing Chen      v1.0.0				添加注释
 */
public class StringUtil {

    public static String stripEscape(String input){
        input=input
                .replaceAll("\\r", "\\\\r")
                .replaceAll("\\t", "\\\\t")
                .replaceAll("\\\\", "\\\\\\\\")
                .replaceAll("\\n", "\\\\\\\\n");
//        input = input.replaceAll("[\\s!$%^*(+\"\']+|[+——！，。？、~#￥%……&*（）]+","");
        return input;
    }

    public static String stripControl(String input){
        return input
                .replaceAll("\\p{Cntrl}","")
                .replaceAll("\\p{InGreek}","");
    }
}
