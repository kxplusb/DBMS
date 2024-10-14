package io.sustc.dto.dependences;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Random;

public class Safeguard {

    public static String getMD5Str(String str) {
        byte[] digest = null;
        str += "salt";
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            digest = md5.digest(str.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        assert digest != null;
        String md5Str = new BigInteger(1, digest).toString(16);
        return md5Str;
    }

    private static short getMD54bv(String str){
        byte[] digest = null;
        str += "salt";
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            digest = md5.digest(str.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        assert digest != null;
        return new BigInteger(1, digest).shortValue();
    }

    //感觉这个不够保险，可以暴力破解
    public static String encodeQQ(String qq){
        if (qq.equals("")){
            return null;
        }
        String s = "qq" + qq + "(o_O;-_-)";
        return getMD5Str(s);
    }
    public static String encodeWechat(String wechat){
        if (wechat.equals("")){
            return null;
        }
        String s = "wechat" + wechat + "(^_^;)";
        return getMD5Str(s);
    }
    public static String encodePassword(String password){
        if(password == null){
            return null;
        }
        return getMD5Str(password);
    }

    public static boolean confirmPassword(String password, String password_db){
        if(password == null){
            return false;
        }
        return encodePassword(password).equals(password_db);
    }
    public static boolean confirmQQ(String qq, String qq_db){
        if(qq == null){
            return true;
        }
        return Objects.equals(encodeQQ(qq), qq_db);
    }
    public static boolean confirmWechat(String WeChat, String WeChat_db){
        if(WeChat == null){
            return true;
        }
        return Objects.equals(encodeWechat(WeChat), WeChat_db);
    }

    public static String generateBV(String seed){
        StringBuilder str = new StringBuilder("BV");

        short num = getMD54bv(seed);
        String hexString = Integer.toHexString(num & 0xffff);
        str.append(hexString);
        while(str.length() < 6){
            str.append("X");
        }


        Random random = new Random();

        for(int i=0; i<6 ;i++){
            char c;
            int type = random.nextInt(3);
            if(type == 0){
                c = (char) ('A' + random.nextInt(26));
            }else if(type == 1){
                c = (char) ('a' + random.nextInt(26));
            }else{
                c = (char) ('1' + random.nextInt(10));
            }
            str.append(c);
        }

        return str.toString();
    }

}
