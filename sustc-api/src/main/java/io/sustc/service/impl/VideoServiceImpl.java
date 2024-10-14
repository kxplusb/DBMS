package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.set.CompositeSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {

    @Autowired
    private DataSource dataSource;

    //todo： 注意即时性的操作都应该尽量在一个sql里面完成并且返回。


    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        //确认auth信息
        //commit_video(post)
        String sql_c = "insert into commit_video where insert into commit_video(bv, operator_id, type, publicTime)" +
                " values (?, ?, 'post', ?);";
        //basic_Info增加一行
        String sql_b = "insert into basicInfo_video(bv, title, description, author_id, duration_sec)" +
                "    VALUES (?, ?, ?, ?, ?);";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long uper;
        if (auth.isValid(conn)){
            uper = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return null;
        }

        String bv = req.getBV();

        try(PreparedStatement stmt_c = conn.prepareStatement(sql_c);
            PreparedStatement stmt_b = conn.prepareStatement(sql_b)){

            stmt_c.setString(1, bv);
            stmt_c.setLong(2, uper);
            stmt_c.setTimestamp(3, req.getPublicTime());
            stmt_c.executeUpdate();

            stmt_b.setString(1,bv);
            stmt_b.setString(2, req.getTitle());
            stmt_b.setString(3, req.getDescription());
            stmt_b.setLong(4, uper);
            stmt_b.setFloat(5, req.getDuration());
            stmt_b.executeUpdate();

            conn.commit();

            return bv;

        } catch (SQLException e) {
            log.info("SQLException: " + e);
            return null;
        }
    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        //确认删除者的信息(作者 or superuser)
        // todo 可以看看能不能写个trigger进数据库，这样就不用在java里面多次判断了（目前java里面还没有判断）

        //commit_video 增加delete行
        String sql_insertDelete = "insert into commit_video(bv, operator_id, type, publicTime)" +
                " values (?, ?, 'delete', now());";

        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt_insertDelete = conn.prepareStatement(sql_insertDelete)){
            conn.setAutoCommit(false);

            long mid;
            if(auth.isValid(conn)){
                mid = auth.getMid();
            }else{
                log.info("auth is invalid when deleteVideo");
                return false;
            }

            stmt_insertDelete.setString(1, bv);
            stmt_insertDelete.setLong(2, mid);
            stmt_insertDelete.executeUpdate();

            conn.commit();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        //确认authInfo//todo 还要确认他有资格修改(直接查commit 或者 last_commit) 应该可以直接上last_commit
        //commit_video 增加update行
        String sql_commit = "insert into commit_video(bv, operator_id, type, publicTime, commitTime)" +
                " values (?, ?, 'update', ?, ?);";
        //update basicInfo_video
        String sql_updateBasicInfo = "update basicInfo_video set title = ?, description = ? where bv = ?;";
        //增加 update_video 行
        String sql_insertUpdate = "insert into update_video(bv, origin_title, origin_description, updateTime)" +
                " VALUES (?, ?, ?, ?)";


        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long uper;
        if (auth.isValid(conn)){
            uper = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return false;
        }

        try(PreparedStatement stmt_commit = conn.prepareStatement(sql_commit);
            PreparedStatement stmt_updateBasicInfo = conn.prepareStatement(sql_updateBasicInfo);
            PreparedStatement stmt_insertUpdate = conn.prepareStatement(sql_insertUpdate)){

            Timestamp t = new Timestamp(System.currentTimeMillis());

            stmt_commit.setString(1, bv);
            stmt_commit.setLong(2, uper);
            stmt_commit.setTimestamp(3, req.getPublicTime());//todo 这里应该要改成last_commit的时间
            stmt_commit.setTimestamp(4, t);
            stmt_commit.executeUpdate();

            stmt_updateBasicInfo.setString(1, req.getTitle());
            stmt_updateBasicInfo.setString(2, req.getDescription());
            stmt_updateBasicInfo.setString(3, bv);
            stmt_updateBasicInfo.executeUpdate();

            stmt_insertUpdate.setString(1, bv);
            stmt_insertUpdate.setString(2, req.getTitle());
            stmt_insertUpdate.setString(3, req.getDescription());
            stmt_insertUpdate.setTimestamp(4, t);
            stmt_insertUpdate.executeUpdate();

            conn.commit();
            return true;
        } catch (SQLException e) {
            log.info("SQLException: " + e);
            return false;
        }

    }


    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        List<String> ans = new ArrayList<>();

        StringBuilder sql = new StringBuilder("");
        String sql_p1 =
            """
                select bv, (length(title) - length(replace(title, ?, '')))/length(?)
                           + (length(description) - length(replace(description, ?, '')))/length(?)
                           + (length(name) - length(replace(name, ?, '')))/length(?)
            """;

        String sql_connector =
            """
               + (length(title) - length(replace(title, ?, '')))/length(?)
               + (length(description) - length(replace(description, ?, '')))/length(?)
               + (length(name) - length(replace(name, ?, '')))/length(?)
            """;

        String sql_superuser_p2 =
            """
                as relevance
                from superuser_video a
                join basicInfo_user b on a.author_id = b.mid
                join (select bv, count(*) as cnt from exist_user_watch_video group by bv) c
                    on a.bv = c.bv
                order by relevance desc, cnt desc ;
            """;

        String sql_user_p2 =
            """
                as relevance
                from (select bv, title, description, author_id from user_video
                        union
                      select bv, title, description, author_id from superuser_video where author_id = ?) a
                join basicInfo_user b on a.author_id = b.mid
                join (select bv, count(*) as cnt from exist_user_watch_video group by bv) c
                    on a.bv = c.bv
                order by relevance desc, cnt desc ;
            """;

        if(keywords == null || keywords.equals("")){
            return ans;
        }
        String[] keys = keywords.split(" ");
        int n = keys.length;
        if(n == 0){
            return ans;
        }


        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            log.info("unexpected cases happened in searchVideo");
            throw new RuntimeException(e);
        }
        long operator;
        if (auth.isValid(conn)){
            operator = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return ans;
        }

        try(PreparedStatement stmt_p1 = conn.prepareStatement(sql_p1);
            PreparedStatement stmt_connector = conn.prepareStatement(sql_connector);
            PreparedStatement stmt_p2 = conn.prepareStatement(sql_user_p2);
            Statement stmt = conn.createStatement()){
            String key = keys[0];

            stmt_p1.setString(1, key);
            stmt_p1.setString(2, key);
            stmt_p1.setString(3, key);
            stmt_p1.setString(4, key);
            stmt_p1.setString(5, key);
            stmt_p1.setString(6, key);
            sql.append(stmt_p1);

            for(int i=1; i<n; i++){
                key = keys[i];
                stmt_connector.setString(1, key);
                stmt_connector.setString(2, key);
                stmt_connector.setString(3, key);
                stmt_connector.setString(4, key);
                stmt_connector.setString(5, key);
                stmt_connector.setString(6, key);
                sql.append(stmt_connector);
            }

            //todo 这里到时候改一下吧，我想着把判断是否是superuser的放进authInfo里面去
            if(isUser(operator)){
                stmt_p2.setLong(1, operator);
                sql.append(stmt_p2);
            }else{
                sql.append(sql_superuser_p2);
            }

            ResultSet rs = stmt.executeQuery(sql.toString());
            if(rs.relative(pageSize*pageNum)){
                int i = 0;
                while (rs.next() && i < pageSize){
                    String bv = rs.getString(1);
                    ans.add(bv);
                    i++;
                }
            }
            return ans;

        } catch (SQLException e) {
            log.info("unexpected cases happened when searchVideo");
            throw new RuntimeException(e);
        }
    }

    @Override
    public double getAverageViewRate(String bv) {
        /*
        select avg((endTime - startTime)/b.duration_sec)
            from exist_user_watch_video e
                join basicInfo_video b on e.bv = b.bv
            where e.bv = 'BV1144y147by';
         */
        String sql =
                """
                select avg((endTime - startTime)/b.duration_sec)
                    from exist_user_watch_video e
                        join basicInfo_video b on e.bv = b.bv
                    where e.bv = ?;
                """;

        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){

            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            float ans = -1;
            if(rs.next()){
                ans = rs.getFloat(1);
                return ans;
            }else{
                log.info("corner cases happens in getAverageViewRate");
                return -1;
            }

        } catch (SQLException e) {
            log.info("unexpected cases happened in getAverageViewRate");
            return -1;
        }
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        /*
        select y.d, y.cnt from (
            select round(videoTime/10 - 0.5) as d, count(*) as cnt from user_danmu
                where bv = 'BV1FZ4y1H7Fz'  group by d order by cnt desc limit 1 ) x
            join (select round(videoTime/10 - 0.5) as d, count(*) as cnt from user_danmu
                where bv = 'BV1FZ4y1H7Fz'  group by d) y
            on x.cnt = y.cnt;
         */
        String sql =
                """
                select y.d, y.cnt from (
                    select round(videoTime/10 - 0.5) as d, count(*) as cnt from user_danmu
                        where bv = ?  group by d order by cnt desc limit 1 ) x
                    join (select round(videoTime/10 - 0.5) as d, count(*) as cnt from user_danmu
                        where bv = ?  group by d) y
                    on x.cnt = y.cnt;
                """;

        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql)){

            stmt.setString(1, bv);
            stmt.setString(2, bv);
            ResultSet rs = stmt.executeQuery();

            Set<Integer> ans = new CompositeSet<>();

            while (rs.next()){
                int x = rs.getInt(1);
                ans.add(x);
            }
            return ans;


        } catch (SQLException e) {
            log.info("unexpected cases happened in getHotspot");
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        //确认auth身份（superuser）
        String sql_isSuperuser = "select identity from exist_user_info where mid = ?;";

        //没被review过，auth不是作者（在not_review 的 view 里面 + 不是作者）
        String sql_notReview = "select count(*) as cnt from not_review where bv = ? and author_id <> ?;";

        //在review里面插入一行
        String sql_insertReview = "insert into review_video(bv, reviewer_id) values (?, ?);";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long reviewer;
        if (auth.isValid(conn)){
            reviewer = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return false;
        }

        try(PreparedStatement stmt_insertReview = conn.prepareStatement(sql_insertReview);
            PreparedStatement stmt_confirmAuthInfo = conn.prepareStatement(sql_isSuperuser);
            PreparedStatement stmt_notReviewed = conn.prepareStatement(sql_notReview)){

            stmt_confirmAuthInfo.setLong(1, reviewer);
            ResultSet rs = stmt_confirmAuthInfo.executeQuery();
            if(rs.next()){
                String s = rs.getString("identity");
                if(!s.equals("superuser")){
                    log.info("reviewer is not a superuser");
                    return false;
                }
            }else {
                log.info("#1 unexpected cases happened in reviewVideo");
                conn.rollback();
                return false;
            }

            stmt_notReviewed.setString(1, bv);
            stmt_notReviewed.setLong(2, reviewer);
            rs = stmt_notReviewed.executeQuery();
            if(rs.next()){
                int x = rs.getInt("cnt");
                if(x==0){
                    log.info("review condition not fulfilled");
                    return false;
                }
            }else {
                log.info("#2 unexpected cases happened in reviewVideo");
                conn.rollback();
                return false;
            }

            stmt_insertReview.setString(1, bv);
            stmt_insertReview.setLong(2, reviewer);
            stmt_insertReview.executeUpdate();
            conn.commit();
            return true;
        } catch (SQLException e) {
            log.info("SQL exception: " + e);
            return false;
        }

    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        //首先要能搜索到且不是作者
        String sql_selectVideo = "select count(*) as cnt from user_video where bv = ? and author_id <> ? ";

        //coin_video表增加一行（必须要之前没coin过，表里面有unique限制可以实现这个）
        String sql_insertCoinVideo = "insert into coin_video(bv, mid) values (?, ?);";

        //level表对应 coin - 1
        String sql_updateCoin = "update level set coin = (select coin from level where mid = ?) - 1" +
                " where mid = ?;";

        //确认coin >= 0
        String coin = "select coin from level where mid = ?;";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long coiner;
        if (auth.isValid(conn)){
            coiner = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return false;
        }

        try(PreparedStatement stmt_selectVideo = conn.prepareStatement(sql_selectVideo);
            PreparedStatement stmt_insertCoinVideo = conn.prepareStatement(sql_insertCoinVideo);
            PreparedStatement stmt_updateCoin = conn.prepareStatement(sql_updateCoin);
            PreparedStatement stmt_coin = conn.prepareStatement(coin)) {

            stmt_selectVideo.setString(1, bv);
            stmt_selectVideo.setLong(2, coiner);
            ResultSet rs = stmt_selectVideo.executeQuery();
            if (rs.next()) {
                int cnt = rs.getInt("cnt");
                if (cnt == 0) {
                    conn.rollback();
                    log.info("corner cases 2 or 3 happens");
                    return false;
                }
            }else {
                log.info("unexpected cases happened in coinVideo");
                conn.rollback();
                return false;
            }

            stmt_insertCoinVideo.setString(1, bv);
            stmt_insertCoinVideo.setLong(2, coiner);
            stmt_insertCoinVideo.executeUpdate();

            stmt_updateCoin.setLong(1, coiner);
            stmt_updateCoin.setLong(2, coiner);
            stmt_updateCoin.executeUpdate();

            stmt_coin.setLong(1, coiner);
            rs = stmt_coin.executeQuery();
            if(rs.next()){
                int c = rs.getInt("coin");
                if(c < 0){
                    conn.rollback();
                    log.info("the man is out of coins");
                    return false;
                }else {
                    conn.commit();
//                    log.info("donate successfully");
                    return true;
                }
            }else{
                log.info("unexpected cases happened in coinVideo");
                conn.rollback();
                return false;
            }


        } catch (SQLException e) {
            log.info("corner cases 4 happens");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                log.info("unexpected cases happened in coinVideo");
            }
            return false;
        }
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        //要能搜索到且不是作者
        String sql_selectVideo = "select count(*) as cnt from user_video where bv = ? and author_id <> ? ";
        //like_video表增加一行
        String sql_insertLikeVideo = "insert into favourite_video(bv, mid) values (?, ?);";
        //获取行数看是不是取消
        String sql_selectLike = "select count(*) as cnt from exist_user_like_video" +
                " where bv = ? and mid = ?";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long liker;
        if (auth.isValid(conn)){
            liker = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return false;
        }

        try(PreparedStatement stmt_selectVideo = conn.prepareStatement(sql_selectVideo);
            PreparedStatement stmt_insertLike = conn.prepareStatement(sql_insertLikeVideo);
            PreparedStatement stmt_selectLike = conn.prepareStatement(sql_selectLike)){
            stmt_selectVideo.setString(1, bv);
            stmt_selectVideo.setLong(2, liker);
            ResultSet rs = stmt_selectVideo.executeQuery();
            if(rs.next()){
                int cnt = rs.getInt("cnt");
                if(cnt == 0){
                    conn.rollback();
                    log.info("corner cases 2 or 3 happens");
                    return false;
                }
            }

            stmt_insertLike.setString(1, bv);
            stmt_insertLike.setLong(2, liker);
            stmt_insertLike.executeUpdate();
            conn.commit();
            stmt_selectLike.setString(1, bv);
            stmt_selectLike.setLong(2, liker);
            rs = stmt_selectLike.executeQuery();
            if(rs.next()){
                int cnt = rs.getInt("cnt");
                return cnt % 2 == 0;
            }else{
                log.info("unexpected cases happened in likeVideo");
                return false;
            }
        } catch (SQLException e) {
            log.info("SQL exception: " + e);
            return false;
        }
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        //要能搜索到且不是作者
        String sql_selectVideo = "select count(*) as cnt from user_video where bv = ? and author_id <> ? ";
        //collect_video表增加一行
        String sql_insertCollectVideo = "insert into collect_video(bv, mid) values (?, ?);";
        //获取行数看是不是取消
        String sql_selectCollect = "select count(*) as cnt from exist_user_collect_video" +
                " where bv = ? and mid = ?";

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long collector;
        if (auth.isValid(conn)){
            collector = auth.getMid();
        }else{
            log.info("corner cases 1 happens");
            return false;
        }

        try(PreparedStatement stmt_selectVideo = conn.prepareStatement(sql_selectVideo);
            PreparedStatement stmt_insertCollect = conn.prepareStatement(sql_insertCollectVideo);
            PreparedStatement stmt_selectCollect = conn.prepareStatement(sql_selectCollect)){
            stmt_selectVideo.setString(1, bv);
            stmt_selectVideo.setLong(2, collector);
            ResultSet rs = stmt_selectVideo.executeQuery();
            if(rs.next()){
                int cnt = rs.getInt("cnt");
                if(cnt == 0){
                    conn.rollback();
                    log.info("corner cases 2 or 3 happens");
                    return false;
                }
            }

            stmt_insertCollect.setString(1, bv);
            stmt_insertCollect.setLong(2, collector);
            stmt_insertCollect.executeUpdate();
            conn.commit();
            stmt_selectCollect.setString(1, bv);
            stmt_selectCollect.setLong(2, collector);
            rs = stmt_selectCollect.executeQuery();
            if(rs.next()){
                int cnt = rs.getInt("cnt");
                return cnt % 2 == 0;
            }else{
                log.info("unexpected cases happened in collectVideo");
                return false;
            }
        } catch (SQLException e) {
            log.info("SQL exception: " + e);
            return false;
        }
    }

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
}
