package com.yanli.flinkdemo.pojo;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 15:33
 */
public class UserBehavior {
    public long userId;
    public long itemId;
    public int categoryId;
    public String behavior;
    public long timestamp;

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }

    public long getUserId() {
        return userId;
    }

    public long getItemId() {
        return itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
