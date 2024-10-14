package io.sustc.dto;

import java.io.Serializable;

import io.sustc.dto.dependences.Safeguard;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The user registration request information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RegisterUserReq implements Serializable {


    public String getPassword() {
        if(password == null || password.equals("")){
            return null;
        }
        return Safeguard.encodePassword(password);
    }

    public String getQq() {
        if(qq == null || qq.equals("")){
            return null;
        }
        return Safeguard.encodeQQ(qq);
    }

    public String getWechat() {
        return Safeguard.encodeWechat(wechat);
    }

    public String getName() {
        return name;
    }

    public String getSex() {
        if(sex == null){
            return null;
        }
        if(sex.equals(Gender.MALE)){
            return "Male";
        } else if (sex.equals(Gender.FEMALE)) {
            return "Female";
        } else if(sex.equals(Gender.UNKNOWN)){
            return "Unknown";
        } else{
            return "Private";
        }
    }

    public String getBirthday() {
        //todo: birthday需要判断是否是日期格式 : x月x日
        return birthday;
    }

    public String getSign() {
        if(sign.equals("")){
            return null;
        }
        return sign;
    }


    private String password;

    private String qq;

    private String wechat;

    private String name;

    private Gender sex;

    private String birthday;

    private String sign;

    public enum Gender {
        MALE,
        FEMALE,
        UNKNOWN,
    }
}
