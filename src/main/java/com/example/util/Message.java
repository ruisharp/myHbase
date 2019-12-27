package com.example.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author luomingkui
 * @date 2018/9/18 下午12:08
 * @desc
 */
public class Message {
    private String uid;
    private String ts;
    private String rowkey;
    private String content;

    public Message() {
    }

    public Message(String uid, String ts, String rowkey, String content) {
        this.uid = uid;
        this.ts = ts;
        this.rowkey = rowkey;
        this.content = content;
    }

    public String getUid() {

        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
    @Override
    public String toString() {
        return "Message{" +
                "用户：'" + uid + '\'' +
                ", 时间：'" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.valueOf(ts)))  + '\'' +
                ", 行键：'" + rowkey + '\'' +
                ", 微博内容：'" + content + '\'' +
                '}' + "\r\n";
    }
}
