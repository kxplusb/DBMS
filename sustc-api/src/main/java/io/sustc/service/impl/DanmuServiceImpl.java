package io.sustc.service.impl;

import com.zaxxer.hikari.pool.HikariProxyResultSet;
import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {

    @Autowired
    private DataSource dataSource;

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        //确认auth
        //确认看过视频
        //generate dmv
        String sql_generateDmv = "select nextval('basicInfo_danmu_mid_seq') as next;";
        //commit 增加行
        String insertCommit = "insert into commit_danmu(dmv, mid) values (?, ?);";
        //basicInfo 增加行
        String insertBasicInfo = "insert into basicInfo_danmu(dmv, bv, mid, content, videoTime)" +
                " VALUES (?, ?, ?, ?, ?);";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        long mid;
        if(auth.isValid(conn)){
            mid = auth.getMid();
        }else {
            log.info("auth is invalid");
            return -1;
        }
        if(!hasWatched(mid, bv, conn)){
            //此人没有看过视频 or 找不到该视频
            log.info("corner cases happens when sendDanmu");
            return -1;
        }
        try(PreparedStatement stmt_generateDMV = conn.prepareStatement(sql_generateDmv);
            PreparedStatement stmt_insertCommit = conn.prepareStatement(insertCommit);
            PreparedStatement stmt_insertBasicInfo = conn.prepareStatement(insertBasicInfo)){

            long dmv;
            ResultSet rs = stmt_generateDMV.executeQuery();
            if(rs.next()){
                dmv = rs.getLong("next");
            }else{
                log.info("unexpected situation #1 in sendDanmu");
                return -1;
            }

            stmt_insertBasicInfo.setLong(1, dmv);
            stmt_insertBasicInfo.setLong(3, mid);
            stmt_insertBasicInfo.setString(2, bv);
            stmt_insertBasicInfo.setString(4, content);
            stmt_insertBasicInfo.setFloat(5, time);
            stmt_insertBasicInfo.executeUpdate();

            stmt_insertCommit.setLong(1, dmv);
            stmt_insertCommit.setLong(2, mid);
            stmt_insertCommit.executeUpdate();

            conn.commit();
            return dmv;

        } catch (SQLException e) {
            log.info("unexpected situation #2 in sendDanmu");
            return -1;
        }
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        List<Long> ans = new ArrayList<>();

        String displayDanmu = "select dmv from user_danmu where videoTime >= ? and videoTime <= ? and bv = ?;";
        String displayDanmu_filtered = "select dmv from user_danmu_filtered" +
                " where videoTime >= ? and videoTime <= ? and bv = ?;";

        //确认time的范围
        String sql_selectVideoDuration = "select duration_sec as ds from user_video where bv = ?;";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try(PreparedStatement stmt_displayDanmu = conn.prepareStatement(displayDanmu);
            PreparedStatement stmt_displayDanmu_f = conn.prepareStatement(displayDanmu_filtered);
            PreparedStatement stmt_selectVideoDuration = conn.prepareStatement(sql_selectVideoDuration)){

            stmt_selectVideoDuration.setString(1, bv);
            ResultSet rs = stmt_selectVideoDuration.executeQuery();
            float f = 0;
            if(rs.next()){
                f = rs.getFloat("ds");
            }else {
                log.info("cannot find the video");
                return null;
            }

            if(timeStart >= timeEnd || timeEnd > f || timeStart < 0){
                log.info("invalid time");
                return null;
            }

            if(filter){
                stmt_displayDanmu_f.setFloat(1, timeStart);
                stmt_displayDanmu_f.setFloat(2, timeEnd);
                stmt_displayDanmu_f.setString(3, bv);
                rs = stmt_displayDanmu_f.executeQuery();
            }else{
                stmt_displayDanmu.setFloat(1, timeStart);
                stmt_displayDanmu.setFloat(2, timeEnd);
                stmt_displayDanmu.setString(3, bv);
                rs = stmt_displayDanmu.executeQuery();
            }
            conn.commit();

            while(rs.next()){
                Long i = rs.getLong("dmv");
                ans.add(i);
            }
            return ans;

        } catch (SQLException e) {
            log.info("exception in displayDanmu" + e);
            return null;
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        //确认auth
        //看了视频
        //like_danmu 增加一行
        String sql = "insert into like_danmu(dmv, mid) VALUES (?, ?);";
        //看看like的状态
        String sql_select = "select count(*) as cnt from like_danmu where dmv = ? and mid = ?;";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        long mid;
        if(auth.isValid(conn)){
            mid = auth.getMid();
        }else {
            log.info("auth is invalid");
            return false;
        }

        //其实这里最好是还可以增加有没有看过这一段（通过watch的时间和弹幕的时间）
        if(!hasWatched(mid, id, conn)){
            //此人没有看过视频 or 找不到该视频 or 没有这条弹幕
            log.info("corner cases happens when likeDanmu");
            return false;
        }

        try(PreparedStatement stmt = conn.prepareStatement(sql);
            PreparedStatement stmt_select = conn.prepareStatement(sql_select)){
            stmt.setLong(1, id);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
            conn.commit();

            stmt_select.setLong(1, id);
            stmt_select.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getInt("cnt") % 2 == 1;
            }else{
                log.info("unexpected cases #1");
                return false;
            }

        } catch (SQLException e) {
            log.info("unexpected cases #2");
            return false;
        }
    }

    public boolean hasWatched(long mid, String bv, Connection conn){
        String sql = "select count(*) as cnt from exist_user_watch_video where bv = ? and mid = ?;";

        try(PreparedStatement stmt = conn.prepareStatement(sql)){

            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getInt("cnt") > 0;
            }else{
                log.info("unexpected issue happens when searching hasWatch");
                return false;
            }

        } catch (SQLException e) {
            log.info("SQL exception: " + e);
            return false;
        }
    }

    public boolean hasWatched(long mid, long dmv, Connection conn){
        String sql = "select count(*) as cnt from exist_user_watch_video" +
                " where bv = (select bv from user_danmu where dmv = ?)" +
                " and mid = ?;";

        try(PreparedStatement stmt = conn.prepareStatement(sql)){

            stmt.setLong(1, dmv);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                return rs.getInt("cnt") > 0;
            }else{
                log.info("unexpected issue happens when searching hasWatch");
                return false;
            }

        } catch (SQLException e) {
            log.info("SQL exception: " + e);
            return false;
        }
    }
}
