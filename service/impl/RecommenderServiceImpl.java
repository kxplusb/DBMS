package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
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
public class RecommenderServiceImpl implements RecommenderService {

    @Autowired
    private DataSource dataSource;
    //todo： 注意即时性的操作都应该尽量在一个sql里面完成并且返回。

    @Override
    public List<String> recommendNextVideo(String bv) {
        //todo 速度略慢,现在大概是4秒左右
        String sql =
                """
                    select a.bv, count(*) as cnt from (select distinct mid from exist_user_watch_video where bv = ?) b
                        join exist_user_watch_video a
                    on a.mid = b.mid where bv <> ? group by a.bv order by cnt desc, bv limit 5;
                """;
        List<String> ans = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, bv);
            stmt.setString(2, bv);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                ans.add(rs.getString(1));
            } else {
                return null;
            }

        } catch (SQLException e) {
            log.info("sqlE: " + e);
            return null;
        }


        throw new UnsupportedOperationException("");
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        //todo 这玩意的速度很不好说，现在大概是分钟级别的，得想想办法
        // 有没有可能提前generate出来一个recommend的表（bv,score），这样子查询的速度就可以上去了
        List<String> ans = new ArrayList<>();
        String sql = "select bv from generalRecommendations limit ? offset ?;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            if (pageSize <= 0 || pageNum <= 0) {
                return null;
            }

            stmt.setInt(1, pageSize);
            stmt.setInt(2, pageSize * pageNum );
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                ans.add(rs.getString(1));
            }
            return ans;

        } catch (SQLException e) {
            log.info("sqlE: " + e);
            return null;
        }
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        //todo 这个现在也要 1.5-2s 的样子，难崩
        List<String> ans = new ArrayList<>();
        String sql =
                """
                    select a.bv from
                    (select e.bv, count(*) as cnt from
                    (select a.followee_id from
                        (select followee_id, follower_id, cnt from
                            (select followee_id, follower_id, count(*) as cnt
                                from exist_follow where follower_id = ?
                                group by followee_id, follower_id) a
                            where cnt % 2 = 1) a
                     join
                        (select followee_id, follower_id, cnt from
                            (select followee_id, follower_id, count(*) as cnt
                                from exist_follow where followee_id = ?
                                group by followee_id, follower_id) a
                            where cnt % 2 = 1) b
                     on a.followee_id = b.follower_id) x
                        join exist_user_watch_video e on e.mid = x.followee_id
                    group by e.bv) a
                    join last_commit_video b on a.bv = b.bv
                    join level c on b.operator_id = c.mid
                    order by a.cnt desc, c.level desc, b.publicTime desc ;
                """;
        if (pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            long mid;
            if (auth.isValid()) {
                mid = auth.getMid();
            } else {
                return null;
            }

            stmt.setLong(1, mid);
            stmt.setLong(2, mid);
            ResultSet rs = stmt.executeQuery();

            if (rs.relative(pageSize * pageNum)) {
                int i = 0;
                while (rs.next() && i < pageSize) {
                    String bv = rs.getString(1);
                    ans.add(bv);
                    i++;
                }
            }
            return ans;

        } catch (SQLException e) {
            log.info("sqlE: " + e);
            return null;
        }
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        //todo 这个现在要 5s的样子，真tm的慢
        // 有点忘了这个对不对了，到时候记得检查一下
        List<Long> ans = new ArrayList<>();
        String sql =
                """
                    select follower_id from
                        (select b.follower_id, count(*) as cnt from
                            (select followee_id, follower_id, cnt from
                                (select followee_id, follower_id, count(*) as cnt
                                    from exist_follow where follower_id = ?
                                    group by followee_id, follower_id) a
                            where cnt % 2 = 1) a
                        join
                            (select followee_id, follower_id, cnt from
                                (select followee_id, follower_id, count(*) as cnt
                                    from exist_follow
                                    group by followee_id, follower_id) a
                            where cnt % 2 = 1) b
                        on a.followee_id = b.followee_id
                        group by b.follower_id) x
                    join level y on x.follower_id = y.mid
                    order by cnt desc, level desc
                    limit ? offset ?;
                """;

        if (pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            long mid;
            if (auth.isValid()) {
                mid = auth.getMid();
            } else {
                return null;
            }

            stmt.setLong(1, mid);
            stmt.setLong(2, mid);
            stmt.setInt(3, pageSize);
            stmt.setInt(4, pageSize * pageNum);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                Long id = rs.getLong(1);
                ans.add(id);
            }
            return ans;

        } catch (SQLException e) {
            log.info("sqlE: " + e);
            return null;
        }
    }
}