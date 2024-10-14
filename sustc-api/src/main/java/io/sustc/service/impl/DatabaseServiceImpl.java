package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
//todo 这个类已经搞好了，剩下要做的就是改下log。
// 还可以改成不用prepareStatement， 估计会快一点（不过不知道能不能用batch了）
// 目前import的时间为4min43s
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    final int batchSize = 100000;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12210305);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());

        Load_User(userRecords);
        Load_Video(videoRecords);
        Load_Danmu(danmuRecords);

    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void Load_User(List<UserRecord> userRecords){

        String sql_insertUserInfo = "insert into basicInfo_user(mid, name, sex, birthday," +
                " sign, registerTime, identity)" +
                " values ( ? , ? , ?, ?, ?, null, ?)";

        String sql_insertLevel = "insert into level(mid, level, coin) values (?, ?, ?)";

        String sql_insertImportantInformation = "insert into importantInformation(mid, password, qq, wechat)" +
                " values (?,?,?,?)";


        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_insertUserInfo = conn.prepareStatement(sql_insertUserInfo);
             PreparedStatement stmt_insertLevel = conn.prepareStatement(sql_insertLevel);
             PreparedStatement stmt_insertImportantInformation = conn.prepareStatement(sql_insertImportantInformation))
        {
            conn.setAutoCommit(false);

            for (UserRecord ur : userRecords) {

                //insertUserInfo
                {
                    stmt_insertUserInfo.setLong(1, ur.getMid());
                    stmt_insertUserInfo.setString(2, ur.getName());
                    stmt_insertUserInfo.setString(3, ur.getSex());
                    stmt_insertUserInfo.setString(4, ur.getBirthday());
                    stmt_insertUserInfo.setString(5, ur.getSign());
                    stmt_insertUserInfo.setString(6, ur.getIdentity());
//                    log.info("SQL: {}", stmt_insertUserInfo);
                    stmt_insertUserInfo.addBatch();
                }

                //insertLevel
                {
                    stmt_insertLevel.setLong(1, ur.getMid());
                    stmt_insertLevel.setInt(2, ur.getLevel());
                    stmt_insertLevel.setInt(3, ur.getCoin());
//                    log.info("SQL: {}", stmt_insertLevel);
                    stmt_insertLevel.addBatch();
                }

                //insertImportantInformation
                {
                    stmt_insertImportantInformation.setLong(1, ur.getMid());
                    stmt_insertImportantInformation.setString(2, ur.getPassword());
                    stmt_insertImportantInformation.setString(3, ur.getQq());
                    stmt_insertImportantInformation.setString(4, ur.getWechat());
//                    log.info("SQL: {}", stmt_insertImportantInformation);
                    stmt_insertImportantInformation.addBatch();
                }
            }
            log.info("initial import user");
            stmt_insertUserInfo.executeBatch();
            stmt_insertUserInfo.clearBatch();
            stmt_insertImportantInformation.executeBatch();
            stmt_insertImportantInformation.clearBatch();
            stmt_insertLevel.executeBatch();
            stmt_insertLevel.clearBatch();

            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        insertFollow(userRecords);
    }

    private void Load_Video(List<VideoRecord> videoRecords){

        String sql_insertBasicInfo = "insert into basicInfo_video(bv, title, description, author_id, duration_sec)" +
                " values (?, ?, ?, ?, ?)";

        //（已改） 到时候还要看一下这里是否需要更改（具体见数据库ddl）
        // 另外导入的数据感觉可以不用进commit表呀（要，因为有publicTime的问题）
        String sql_insertCommit = "insert into commit_video(bv, operator_id, " +
                " type, publicTime, commitTime)" +
                " values (?, ?, 'post', ?, ?)";

        String sql_insertReview = "insert into review_video(bv, reviewer_id, reviewTime) values (?, ?, ?);";

        String sql_insertFavourite = "insert into favourite_video(bv, mid, favourTime) " +
                "values (?, ?, null)";
        String sql_insertCollect = "insert into collect_video(bv, mid, collectTime) " +
                "values (?, ?, null)";
        String sql_insertCoin = "insert into coin_video(bv, mid, coinTime) " +
                "values (?, ?, null)";

        String sql_insertWatch = "insert into watch_video(bv, mid, endTime, watchTime) values (?, ?, ?, null)";


        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_insertBasicInfo = conn.prepareStatement(sql_insertBasicInfo);
             PreparedStatement stmt_insertCommit = conn.prepareStatement(sql_insertCommit);
             PreparedStatement stmt_insertReview = conn.prepareStatement(sql_insertReview);
             PreparedStatement stmt_insertFavourite = conn.prepareStatement(sql_insertFavourite);
             PreparedStatement stmt_insertCollect = conn.prepareStatement(sql_insertCollect);
             PreparedStatement stmt_insertCoin = conn.prepareStatement(sql_insertCoin);
             PreparedStatement stmt_insertWatch = conn.prepareStatement(sql_insertWatch))
        {
            conn.setAutoCommit(false);

            //基本信息
            for (VideoRecord videoRecord : videoRecords) {
                stmt_insertBasicInfo.setString(1, videoRecord.getBv());
                stmt_insertBasicInfo.setString(2, videoRecord.getTitle());
                stmt_insertBasicInfo.setString(3, videoRecord.getDescription());
                stmt_insertBasicInfo.setLong(4, videoRecord.getOwnerMid());
                stmt_insertBasicInfo.setFloat(5, videoRecord.getDuration());
//                log.info("SQL: {}", stmt_insertBasicInfo);
                stmt_insertBasicInfo.addBatch();
            }
            log.info("SQL: {}", "execute batch: " + stmt_insertBasicInfo);
            stmt_insertBasicInfo.executeBatch();
            stmt_insertBasicInfo.clearBatch();
            stmt_insertBasicInfo.close();

            //投稿信息
            for (VideoRecord videoRecord : videoRecords) {
                stmt_insertCommit.setString(1, videoRecord.getBv());
                stmt_insertCommit.setLong(2, videoRecord.getOwnerMid());
                stmt_insertCommit.setTimestamp(3, videoRecord.getPublicTime());
                stmt_insertCommit.setTimestamp(4, videoRecord.getCommitTime());
//                log.info("SQL: {}", stmt_insertCommit);
                stmt_insertCommit.addBatch();
            }
            log.info("SQL: {}", "execute batch: " + stmt_insertCommit);
            stmt_insertCommit.executeBatch();
            stmt_insertCommit.clearBatch();
            stmt_insertCommit.close();

            //审核信息
            for (VideoRecord videoRecord : videoRecords) {
                stmt_insertReview.setString(1, videoRecord.getBv());
                stmt_insertReview.setLong(2, videoRecord.getReviewer());
                stmt_insertReview.setTimestamp(3, videoRecord.getReviewTime());
//                log.info("SQL: {}", stmt_insertReview);
                stmt_insertReview.addBatch();
            }
            log.info("SQL: {}", "execute batch: " + stmt_insertReview);
            stmt_insertReview.executeBatch();
            stmt_insertReview.clearBatch();
            stmt_insertReview.close();

            //点赞信息
            int cnt = 0;
            for (VideoRecord videoRecord : videoRecords) {
                long[] a = videoRecord.getLike();
                stmt_insertFavourite.setString(1, videoRecord.getBv());
                for (long l : a) {
                    stmt_insertFavourite.setLong(2, l);
//                    log.info("SQL: {}", stmt_insertFavourite);
                    stmt_insertFavourite.addBatch();
                    cnt++;
                }
                if(cnt > batchSize) {
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertFavourite);
                    stmt_insertFavourite.executeBatch();
                    stmt_insertFavourite.clearBatch();
                }
            }
            cnt = 0;
            stmt_insertFavourite.executeBatch();
            stmt_insertFavourite.clearBatch();
            stmt_insertFavourite.close();
            conn.commit();

            //投币信息
            for (VideoRecord videoRecord : videoRecords) {
                long[] a = videoRecord.getCoin();
                stmt_insertCoin.setString(1, videoRecord.getBv());
                for (long l : a) {
                    stmt_insertCoin.setLong(2, l);
//                    log.info("SQL: {}", stmt_insertCoin);
                    stmt_insertCoin.addBatch();
                    cnt++;
                }
                if(cnt > batchSize) {
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertCoin);
                    stmt_insertCoin.executeBatch();
                    stmt_insertCoin.clearBatch();
                }
            }
            cnt = 0;
            stmt_insertCoin.executeBatch();
            stmt_insertCoin.clearBatch();
            stmt_insertCoin.close();
            conn.commit();

            //收藏信息
            for (VideoRecord videoRecord : videoRecords) {
                long[] a = videoRecord.getFavorite();
                stmt_insertCollect.setString(1, videoRecord.getBv());
                for (long l : a) {
                    stmt_insertCollect.setLong(2, l);
//                    log.info("SQL: {}", stmt_insertCollect);
                    stmt_insertCollect.addBatch();
                    cnt++;
                }
                if(cnt > batchSize) {
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertCollect);
                    stmt_insertCollect.executeBatch();
                    stmt_insertCollect.clearBatch();
                }
            }
            cnt = 0;
            log.info("SQL: {}", "execute batch:" + stmt_insertCollect);
            stmt_insertCollect.executeBatch();
            stmt_insertCollect.clearBatch();
            stmt_insertCollect.close();
            conn.commit();

            //观看信息
            for (VideoRecord videoRecord : videoRecords){
                stmt_insertWatch.setString(1, videoRecord.getBv());
                long[] l = videoRecord.getViewerMids();
                float[] t = videoRecord.getViewTime();
                int m = l.length;
                for(int i=0;i<m;i++){
//                    log.info("SQL: {}", stmt_insertWatch);
                    stmt_insertWatch.setLong(2, l[i]);
                    stmt_insertWatch.setFloat(3, t[i]);
                    stmt_insertWatch.addBatch();
                    cnt++;
                }
                if(cnt > batchSize) {
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertWatch);
                    stmt_insertWatch.executeBatch();
                    stmt_insertWatch.clearBatch();
                }
            }
            log.info("SQL: {}", "execute batch:" + stmt_insertWatch);
            stmt_insertWatch.executeBatch();
            stmt_insertWatch.clearBatch();
            conn.commit();

        }
        catch (SQLException e) {
            log.info("sqlE: " + e);
            throw new RuntimeException(e);
        }
    }

    private void Load_Danmu(List<DanmuRecord> danmuRecords){
        String sql_insertBasicInfoDanmu = "insert into basicInfo_danmu(dmv, bv, mid, content, videoTime)" +
                " values (?, ?, ?, ?, ?)";
        String sql_insertCommitDanmu = "insert into commit_danmu(dmv, mid, commitTime) values (?, ?, ?)";
        String sql_insertLikeDanmu = "insert into like_danmu(dmv, mid, likeTime) VALUES (?, ?, null)";


        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt_insertBasicInfoDanmu = conn.prepareStatement(sql_insertBasicInfoDanmu);
            PreparedStatement stmt_insertCommitDanmu = conn.prepareStatement(sql_insertCommitDanmu);
            PreparedStatement stmt_insertLikeDanmu = conn.prepareStatement(sql_insertLikeDanmu)) {

            conn.setAutoCommit(false);

            long dmv = 0;
            for (DanmuRecord dmr : danmuRecords) {

                //这个不能用时间来表示，因为太多了
                dmv++;

                //基本信息
                stmt_insertBasicInfoDanmu.setLong(1, dmv);
                stmt_insertBasicInfoDanmu.setString(2, dmr.getBv());
                stmt_insertBasicInfoDanmu.setLong(3, dmr.getMid());
                stmt_insertBasicInfoDanmu.setString(4, dmr.getContent());
                stmt_insertBasicInfoDanmu.setFloat(5, dmr.getTime());
//                log.info("SQL: {}", stmt_insertBasicInfoDanmu);
                stmt_insertBasicInfoDanmu.addBatch();

                //发送弹幕信息
                stmt_insertCommitDanmu.setLong(1, dmv);
                stmt_insertCommitDanmu.setLong(2, dmr.getMid());
                stmt_insertCommitDanmu.setTimestamp(3, dmr.getPostTime());
//                log.info("SQL: {}", stmt_insertCommitDanmu);
                stmt_insertCommitDanmu.addBatch();
            }
            log.info("SQL: {}", "execute batch:" + stmt_insertBasicInfoDanmu);
            stmt_insertBasicInfoDanmu.executeBatch();
            log.info("SQL: {}", "execute batch:" + stmt_insertCommitDanmu);
            stmt_insertCommitDanmu.executeBatch();

            dmv = 0;
            int cnt = 0;
            for (DanmuRecord dmr : danmuRecords) {
                dmv++;
                stmt_insertLikeDanmu.setLong(1, dmv);
                long[] l = dmr.getLikedBy();
                for (long value : l) {
                    stmt_insertLikeDanmu.setLong(2, value);
//                    log.info("SQL: {}", stmt_insertLikeDanmu);
                    stmt_insertLikeDanmu.addBatch();
                    cnt++;
                }

                if(cnt > batchSize) {
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertLikeDanmu);
                    stmt_insertLikeDanmu.executeBatch();
                    stmt_insertLikeDanmu.clearBatch();
                }
            }
            log.info("SQL: {}", "execute batch:" + stmt_insertLikeDanmu);
            stmt_insertLikeDanmu.executeBatch();
            stmt_insertLikeDanmu.clearBatch();

            conn.commit();
        }catch (SQLException e) {
            log.info("sqlE: " + e);
            throw new RuntimeException(e);
        }
    }


    private void insertFollow(List<UserRecord> userRecords){
        String sql_insertFollow = "insert into follow(follower_id, followee_id, followTime) values (?,?,null)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt_insertFollow = conn.prepareStatement(sql_insertFollow)){

            conn.setAutoCommit(false);

            int cnt = 0;
            for (UserRecord userRecord : userRecords) {
                stmt_insertFollow.setLong(1, userRecord.getMid());
                long[] follower = userRecord.getFollowing();
                for (long l : follower) {
                    stmt_insertFollow.setLong(2, l);
//                    log.info("SQL: {}", stmt_insertFollow);

                    cnt++;
                    stmt_insertFollow.addBatch();
                }

                if(cnt > batchSize){
                    cnt = 0;
                    log.info("SQL: {}", "execute batch:" + stmt_insertFollow);
                    stmt_insertFollow.executeBatch();
                    stmt_insertFollow.clearBatch();
                }
            }
            stmt_insertFollow.executeBatch();
            stmt_insertFollow.clearBatch();

            conn.commit();
        }catch (SQLException e) {
            log.info("sqlE: " + e);
            throw new RuntimeException(e);
        }
    }

}
