package io.sustc.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The user information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserInfoResp implements Serializable {


    public void setMid(long mid) {
        this.mid = mid;
    }

    public void setCoin(int coin) {
        this.coin = coin;
    }

    public void setFollowing(long[] following) {
        this.following = following;
    }

    public void setFollower(long[] follower) {
        this.follower = follower;
    }

    public void setWatched(String[] watched) {
        this.watched = watched;
    }

    public void setLiked(String[] liked) {
        this.liked = liked;
    }

    public void setCollected(String[] collected) {
        this.collected = collected;
    }

    public void setPosted(String[] posted) {
        this.posted = posted;
    }


    /**
     * The user's {@code mid}.
     */
    private long mid;

    /**
     * The number of user's coins that he/she currently owns.
     */
    private int coin;

    /**
     * The user's following {@code mid}s.
     */
    private long[] following;

    /**
     * The user's follower {@code mid}s.
     */
    private long[] follower;

    /**
     * The videos' {@code bv}s watched by this user.
     */
    private String[] watched;

    /**
     * The videos' {@code bv}s liked by this user.
     */
    private String[] liked;

    /**
     * The videos' {@code bv}s collected by this user.
     */
    private String[] collected;

    /**
     * The videos' {@code bv}s posted by this user.
     */
    private String[] posted;
}
