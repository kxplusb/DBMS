package io.sustc.dto;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.sustc.dto.dependences.Safeguard;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The authorization information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthInfo implements Serializable {


    public long getMid() {
        System.out.println("the auth mid is: " + mid);
        return mid;
    }

    //此处必须要先判断isValid然后才能getMid,因为传进来的authInfo不一定有mid，可能需要通过wechat/qq获取mid
    public boolean isValid(Connection conn){

        if(hasConfirmed){
            return isValid;
        }

        long mid_wechat;
        long mid_qq;

        if(wechat == null){
            if(qq == null){
                if(password == null){
                    isValid = false;
                    hasConfirmed = true;
                    return false;
                }else {
                    isValid = Safeguard.confirmPassword(password, selectPasswordByMid(conn));
                    hasConfirmed = true;
                    return isValid;
                }
            }
            else{
                mid_qq = selectMidByQQ(conn);
                if(mid == 0 || mid == mid_qq){
                    mid = mid_qq;
                    hasConfirmed = true;
                    isValid = true;
                    return true;
                }
                hasConfirmed = true;
                isValid = false;
                return false;
            }
        }
        else{
            mid_wechat = selectMidByWechat(conn);
            if(qq == null){
                if(mid == 0 || mid == mid_wechat){
                    mid = mid_wechat;
                    hasConfirmed = true;
                    isValid = true;
                    return true;
                }
                hasConfirmed = true;
                isValid = false;
                return false;
            }else {
                mid_qq = selectMidByQQ(conn);
                if(mid == 0 && mid_qq == mid_wechat){
                    mid = mid_qq;
                    hasConfirmed = true;
                    isValid = true;
                    return true;
                } else if (mid == mid_qq && mid_qq == mid_wechat) {
                    hasConfirmed = true;
                    isValid = true;
                    return true;
                } else{
                    hasConfirmed = true;
                    isValid = false;
                    return false;
                }
            }
        }
    }

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;

    public void setHasConfirmed(boolean hasConfirmed) {
        this.hasConfirmed = hasConfirmed;
    }

    @Builder.Default
    private boolean hasConfirmed = false;


    private boolean isValid;

    private long selectMidByQQ(Connection conn) {
        String sql = "select mid from importantInformation where qq = ?";
        ResultSet rs;
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1, Safeguard.encodeQQ(qq));
            rs = stmt.executeQuery();
            return rs.getLong("mid");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long selectMidByWechat(Connection conn) {
        String sql = "select mid from importantInformation where wechat = ?";
        ResultSet rs;
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1, Safeguard.encodeWechat(wechat));
            rs = stmt.executeQuery();
            return rs.getLong("mid");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String selectPasswordByMid(Connection conn) {
        String sql = "select password from importantInformation where mid = ?";
        ResultSet rs;
        try(PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, mid);
            rs = stmt.executeQuery();
            return rs.getString("password");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
