package com.yanli.flinkdemo.pojo;

/**
 * @author YanLi
 * @version 1.0
 * @date 2019-08-16 15:59
 * 商品点击量
 */
public class ItemViewCount {
    public long itemId;
    public long windowEnd;
    public long viewCount;

    public ItemViewCount() {
    }

    public ItemViewCount(long itemId, long windowEnd, long viewCount) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public long getItemId() {
        return itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }
    public static ItemViewCount getInstance(long itemId,long windowEnd,long viewCount){
        ItemViewCount  it = new ItemViewCount(itemId,windowEnd,viewCount);
        return it;
    }
}
