package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    //已完成，后续看看能不能改一下报错信息，使得报错信息更有针对性
    //根据目前的信息，名字可以重复,但是微信以及qq的账号仍然应当是unique的，目前是直接删除impoInfo对应行
    @Override
    public long register(RegisterUserReq req) {
        String sql_insertUserInfo = "insert into basicInfo_user(mid, name, sex, birthday, sign)" +
                " values (?, ? , ?, ?, ?)";

        String sql_insertLevel = "insert into level(mid) values (?)";

        String sql_insertImportantInformation = "insert into importantInformation(mid, password, qq, wechat)" +
                " values (?,?,?,?)";

        String sql_selectMid = "select nextval('basicInfo_user_mid_seq') as next";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (
                PreparedStatement stmt_insertUserInfo = conn.prepareStatement(sql_insertUserInfo);
                Statement stmt = conn.createStatement();
                PreparedStatement stmt_insertLevel = conn.prepareStatement(sql_insertLevel);
                PreparedStatement stmt_insertImportantInformation = conn.prepareStatement(sql_insertImportantInformation))
        {
            conn.setAutoCommit(false);

//            log.info("SQL: {}", sql_selectMid);
            ResultSet rs = stmt.executeQuery(sql_selectMid);
            long mid = 0;
            if(rs.next()) {
                mid = rs.getLong("next");
            }

            stmt_insertUserInfo.setLong(1, mid);
            stmt_insertUserInfo.setString(2, req.getName());
            stmt_insertUserInfo.setString(3, req.getSex());
            stmt_insertUserInfo.setString(4, req.getBirthday());
            stmt_insertUserInfo.setString(5, req.getSign());
//            log.info("SQL: {}", stmt_insertUserInfo);
            stmt_insertUserInfo.executeUpdate();

            stmt_insertLevel.setLong(1,mid);
//            log.info("SQL: {}", stmt_insertLevel);
            stmt_insertLevel.executeUpdate();

            stmt_insertImportantInformation.setLong(1,mid);
            stmt_insertImportantInformation.setString(2,req.getPassword());
            stmt_insertImportantInformation.setString(3,req.getQq());
            stmt_insertImportantInformation.setString(4,req.getWechat());
//            log.info("SQL: {}", stmt_insertImportantInformation);
            stmt_insertImportantInformation.executeUpdate();

            log.info("\ncommit: " + stmt_insertUserInfo + "\n" + stmt_insertLevel
                    + "\n" + stmt_insertImportantInformation);
            conn.commit();

            conn.close();
            return mid;

        } catch (SQLException e) {
            log.info("SQLException: " + e);
            try {
                conn.rollback();
                conn.close();
            } catch (SQLException ex) {
                log.info("SQLException: rollback failed because of " + e);
            }
            return -1;
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        //先确认authInfo的信息
        //在删除表中增加行
        String sql_insertDelete = "insert into deleteAccount(mid, mid_operator) values (?, ?);";
        //删掉importantInfo中的信息
        String sql_delete = "delete from importantInformation where mid = ?";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        long user_mid;

        if(auth.isValid(conn)){
            user_mid = auth.getMid();
        }else {
            return false;
        }

        //判断合法性
        //todo 我在想，这个东西写进trigger里面会不会更好一点，毕竟写进trigger就不用多和java交互这一次
        // 尽量写trigger吧
        if((user_mid != mid) && (isUser(user_mid) || !isUser(mid))){
            log.info(user_mid + " cannot delete " + mid);
            return false;
        }

        try(PreparedStatement stmt_insertDelete = conn.prepareStatement(sql_insertDelete);
            PreparedStatement stmt_delete = conn.prepareStatement(sql_delete)){

            stmt_insertDelete.setLong(1, mid);
            stmt_insertDelete.setLong(2, user_mid);
            stmt_insertDelete.executeUpdate();

            stmt_delete.setLong(1, mid);
            stmt_delete.executeUpdate();

            log.info(user_mid + " deleted " + mid);
            conn.commit();
            auth.setHasConfirmed(false);
            return true;

        } catch (SQLException e) {
            log.info("sql exception: " + e);
            try {
                conn.rollback();
            } catch (SQLException ex) {
                log.info("exception happened when executing rollback");
            }
            return false;
        }

    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        String sql_insertFollow = "insert into follow(follower_id, followee_id) values (?, ?)";
        String sql_selectFollow = "select count(*) as cnt from follow where follower_id = ? and followee_id = ?";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        long follower_mid;

        if(auth.isValid(conn)){
            follower_mid = auth.getMid();
//            //不能自己follow自己
//            if( follower_mid == followeeMid){
//                return false;
//            }
        }else {
            return false;
        }

        try(PreparedStatement stmt_insertFollow = conn.prepareStatement(sql_insertFollow);
            PreparedStatement stmt_selectFollow = conn.prepareStatement(sql_selectFollow)){

            stmt_insertFollow.setLong(1, follower_mid);
            stmt_insertFollow.setLong(2, followeeMid);
            stmt_insertFollow.executeUpdate();
            conn.commit();

            //此处return的是follow的状态，因此还需要做一次查询
            stmt_selectFollow.setLong(1, follower_mid);
            stmt_selectFollow.setLong(2, followeeMid);
            ResultSet rs = stmt_selectFollow.executeQuery();
            if(rs.next()) {
                int cnt = rs.getInt("cnt");
                if(cnt % 2 == 0){
                    log.info(follower_mid + " unfollowed " + followeeMid);
                    return false;
                }else{
                    log.info(follower_mid + " followed " + followeeMid);
                    return true;
                }
            }else {
                return false;
            }

        } catch (SQLException e) {
            log.info("sql exception: " + e);
            try {
                conn.rollback();
            } catch (SQLException ex) {
                log.info("exception happened when executing rollback");
            }
            return false;
        }

    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        UserInfoResp rir = new UserInfoResp();
        rir.setMid(mid);

        String sql_select = "select coin from exist_user_info where mid = " + mid;
        //因为我只要单数的follow，所以要写个group进来才行
        String sql_selectFollowee = "select followee_id as mid, count(*) as cnt from exist_follow " +
                "where follower_id = ? group by followee_id";
        String sql_selectFollower = "select follower_id as mid, count(*) as cnt from exist_follow " +
                "where followee_id = ? group by follower_id";
        String sql_selectWatch = "select wv.bv as bv from watch_video wv join superuser_video sv on wv.bv = sv.bv" +
                " where mid = " + mid ;
        String sql_selectLike = "select fv.bv, count(*) from (select bv from favourite_video where mid = " + mid + ")" +
                " fv join superuser_video sv on fv.bv = sv.bv group by fv.bv;";
        String sql_selectCollect = "select cv.bv, count(*) from (select bv from collect_video where mid = " + mid + ")" +
                " cv join superuser_video sv on cv.bv = sv.bv group by cv.bv;";
        String sql_selectPosted = "select bv from superuser_video where author_id = " + mid;

        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            PreparedStatement stmt_selectFollowee = conn.prepareStatement(sql_selectFollowee);
            PreparedStatement stmt_selectFollower = conn.prepareStatement(sql_selectFollower);){
            ResultSet rs = stmt.executeQuery(sql_select);
            int i;

            //getCoin
            if(rs.next()){
                rir.setCoin(rs.getInt("coin"));
            }else{
                return null;
            }

            //followee
            stmt_selectFollowee.setLong(1, mid);
            rs = stmt_selectFollowee.executeQuery();
            ArrayList<Long> followee = new ArrayList<>();
            while (rs.next()){
                int cnt = rs.getInt("cnt");
                if(cnt % 2 == 1){
                    followee.add(rs.getLong("mid"));
                }
            }
            rir.setFollowing(lTa(followee));

            //follower
            stmt_selectFollower.setLong(1, mid);
            rs = stmt_selectFollower.executeQuery();
            ArrayList<Long> follower = new ArrayList<>();
            while (rs.next()){
                int cnt = rs.getInt("cnt");
                if(cnt % 2 == 1){
                    follower.add(rs.getLong("mid"));
                }
            }
            rir.setFollower(lTa(follower));

            //watch
            ArrayList<String> ans = new ArrayList<>();
            rs = stmt.executeQuery(sql_selectWatch);
            while (rs.next()){
                ans.add(rs.getString("bv"));
            }
            rir.setWatched(lTa(ans,"str"));

            //like
            ans = new ArrayList<>();
            rs = stmt.executeQuery(sql_selectLike);
            while (rs.next()){
                ans.add(rs.getString("bv"));
            }
            rir.setLiked(lTa(ans,"str"));

            //collect
            ans = new ArrayList<>();
            rs = stmt.executeQuery(sql_selectCollect);
            while (rs.next()){
                ans.add(rs.getString("bv"));
            }
            rir.setCollected(lTa(ans,"str"));

            //posted
            ans = new ArrayList<>();
            rs = stmt.executeQuery(sql_selectPosted);
            while (rs.next()){
                ans.add(rs.getString("bv"));
            }
            rir.setPosted(lTa(ans,"str"));

            return rir;


        } catch (SQLException e) {
            log.info("oops, something went wrong when getting the user's information!");
            return null;
        }
    }


    /**
     * 因为auth一定是正确的，因此此处如果是因为找不到用户而return false的话那么上一层的判断就不会成功
     *
     * @param mid 用户的mid
     * @return 如果用户是superuser，则返回false，
     * 如果用户是user，则返回true
     */
    private boolean isUser(long mid){
        //此处的表格用的是view
        String sql = "select identity as id from exist_user_info where mid = " + mid;
        try(Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();){
            ResultSet rs = stmt.executeQuery(sql);
            if(rs.next()){
                return rs.getString("id").equals("user");
            }
        } catch (SQLException e) {
            log.info("there is no such user of mid " + mid);
            return false;
        }
        log.info("there is no such user of mid " + mid);
        return false;
    }

    private long[] lTa(ArrayList<Long> x){
        int n = x.size();
        long[] f = new long[n];
        for(int i=0;i<n;i++){
            f[i] = x.get(i);
        }
        return f;
    }
    private String[] lTa(ArrayList<String> x, String y){
        int n = x.size();
        String[] f = new String[n];
        for(int i=0; i<n; i++){
            f[i] = x.get(i);
        }
        return f;
    }
}
