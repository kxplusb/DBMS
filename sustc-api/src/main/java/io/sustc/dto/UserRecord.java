package io.sustc.dto;

import java.io.Serializable;

import io.sustc.dto.dependences.Safeguard;
import lombok.Data;

/**
 * The user record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
public class UserRecord implements Serializable {

    public long getMid() {
        return mid;
    }

    public String getName() {
        if(name.equals("")){
            return null;
        }
        return name;
    }

    public String getSex() {
        if(sex == null){
            return "Unknown";
        } else if(sex.equals("Male") || sex.equals("Female") || sex.equals("Private") || sex.equals("Unknown")) {
            return sex;
        } else {
            if (sex.equals("男") || sex.equals("MALE") || sex.equals("male")) {
                return "Male";
            } else if (sex.equals("女") || sex.equals("FEMALE") || sex.equals("female")) {
                return "Female";
            } else if (sex.equals("保密") || sex.equals("PRIVATE") || sex.equals("private")) {
                return "Private";
            } else if (sex.equals("未知") || sex.equals("UNKNOWN") || sex.equals("unknown")) {
                return "Unknown";
            } else {
                return "Others";
            }
        }
    }

    public String getBirthday() {
        if(birthday.equals("")){
            return null;
        }
        return birthday;
    }

    public short getLevel() {
        return level;
    }

    public int getCoin(){
        return coin;
    }

    public String getSign() {
        if(sign.equals("")){
            return null;
        }
        return sign;
    }

    public long[] getFollowing() {
        return following;
    }

    public String getIdentity() {
        if(identity.equals(Identity.USER)){
            return "user";
        }else if(identity.equals(Identity.SUPERUSER)){
            return "superuser";
        }
        return null;
    }

    public String getPassword() {
        if(password == null){
            return null;
//            throw new RuntimeException("password cannot be null");
        }
        return Safeguard.encodePassword(password);
    }

    public String getQq() {
        if(qq == null){
            return null;
        }else{
            return Safeguard.encodeQQ(qq);
        }
    }

    public String getWechat() {
        if(wechat == null){
            return null;
        }else{
            return Safeguard.encodeWechat(wechat);
        }
    }


    /**
     * The user's unique ID
     */
    private long mid;

    /**
     * The user's name
     */
    private String name;

    /**
     * The user's sex
     */
    private String sex;

    /**
     * The user's birthday, can be empty
     */
    private String birthday;

    /**
     * The user's level
     */
    private short level;

    /**
     * The user's current number of coins
     */
    private int coin;

    /**
     * The user's personal sign, can be null or empty
     */
    private String sign;

    /**
     * The user's identity
     */
    private Identity identity;

    /**
     * The user's password
     */
    private String password;

    /**
     * The user's unique qq, may be null or empty (not unique when null or empty)
     */
    private String qq;

    /**
     * The user's unique wechat, may be null or empty (not unique when null or empty)
     */
    private String wechat;

    /**
     * The users' {@code mid}s who are followed by this user
     */
    private long[] following;

    public enum Identity {
        USER,
        SUPERUSER,
    }
}
