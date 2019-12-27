package weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author luomingkui
 * @date 2018/9/18 下午12:09
 * @desc
 * * 初始化：
 * 1、创建命名空间
 * 2、创建3张表
 *
 * 业余逻辑：
 * 1、发布微博
 * 2、删除微博
 * 3、关注
 * 4、取关
 * 5、展示微博内容
 */
public class Weibo {

    //初始化Configuration
    private static Configuration conf = null;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.204");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    //声明3张表名
    private static final byte[] TABLE_USER = Bytes.toBytes("ns_weibo:user");
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("ns_weibo:content");
    private static final byte[] TABLE_RELATION = Bytes.toBytes("ns_weibo:relation");
    private static final byte[] TABLE_INBOX = Bytes.toBytes("ns_weibo:inbox");


    //定义初始化方法
    private void init() throws IOException {
        //创建命名空间
        initNamespace();
        //创建3张表
        createTableUser();
        createTableContent();
        createTableRelation();
        createTableInbox();
    }

    /**
     * 初始化命名空间
     */
    private void initNamespace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_weibo")
                .addConfiguration("creator", "RUI")
                .addConfiguration("create_ts", String.valueOf(System.currentTimeMillis()))
                .build();
        admin.createNamespace(ns_weibo);
        admin.close();
    }

    private  void createTableUser() throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建表描述器
        HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_USER));
        //创建列描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //2M
        infoColumnDescriptor.setBlocksize(2097152);
        //设置版本确界
        infoColumnDescriptor.setMaxVersions(1).setMinVersions(1);

        //将列描述器，添加到表描述器中
        contentTableDescriptor.addFamily(infoColumnDescriptor);
        //根据表描述器创建表
        admin.createTable(contentTableDescriptor);
        if(admin != null)
            admin.close();
        if(conn != null && !conn.isClosed())
            conn.close();
    }

    /**
     * 创建微博内容表
     * 表名：ns_weibo:content
     * RowKey：uid_timestamp
     * 列族：info
     * 列名：content
     * 值：String类型的微博内容
     */
    private void createTableContent() throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        //创建表描述器
        HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));
        //创建列描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //2M
        infoColumnDescriptor.setBlocksize(2097152);
        //设置版本确界
        infoColumnDescriptor.setMaxVersions(1).setMinVersions(1);

        //将列描述器，添加到表描述器中
        contentTableDescriptor.addFamily(infoColumnDescriptor);
        //根据表描述器创建表
        admin.createTable(contentTableDescriptor);
        if(admin != null)
            admin.close();
        if(conn != null && !conn.isClosed())
            conn.close();
    }

    /**
     * 创建微博用户关系表
     * 表名：ns_weibo:relation
     * RowKey：uid
     * 列族：attends，fans
     * 列名：uid
     * 值：uid（或不填充值）
     */
    private void createTableRelation() throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        //创建表描述器
        HTableDescriptor relationTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_RELATION));
        //创建列描述器
        HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor("attends");
        HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor("fans");
        //设置块缓存
        attendsColumnDescriptor.setBlockCacheEnabled(true);
        fansColumnDescriptor.setBlockCacheEnabled(true);
        //2M
        attendsColumnDescriptor.setBlocksize(2097152);
        fansColumnDescriptor.setBlocksize(2097152);
        //设置版本确界
        attendsColumnDescriptor.setMaxVersions(1).setMinVersions(1);
        fansColumnDescriptor.setMaxVersions(1).setMinVersions(1);

        //将列描述器，添加到表描述器中
        relationTableDescriptor.addFamily(attendsColumnDescriptor);
        relationTableDescriptor.addFamily(fansColumnDescriptor);
        //根据表描述器创建表
        admin.createTable(relationTableDescriptor);
        if(admin != null)
            admin.close();
        if(conn != null && !conn.isClosed())
            conn.close();
    }

    /**
     * 创建微博收件箱表
     * 表名：ns_weibo:inbox
     * RowKey：uid
     * 列族：info
     * 列名：uid（关注的人的uid）
     * 值：关注的人发布的微博的rowkey信息
     */
    private void createTableInbox() throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建表描述器
        HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));
        //创建列描述器
        HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor("info");
        //设置块缓存
        infoColumnDescriptor.setBlockCacheEnabled(true);
        //2M
        infoColumnDescriptor.setBlocksize(2097152);
        //设置版本确界
        infoColumnDescriptor.setMaxVersions(1000).setMinVersions(1000);

        //将列描述器，添加到表描述器中
        inboxTableDescriptor.addFamily(infoColumnDescriptor);
        //根据表描述器创建表
        admin.createTable(inboxTableDescriptor);
        if(admin != null)
            admin.close();
        if(conn != null && !conn.isClosed())
            conn.close();
    }

    /**
     * 发布微博
     * a、将微博内容表中的数据 + 1
     * b、向粉丝微博收件箱表中添加这一条发布的微博的rowkey
     */

    public void publishContent(String uid, String content) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table contentTable = conn.getTable(TableName.valueOf(TABLE_CONTENT));
        //a
        long timestamp = System.currentTimeMillis();
        String rowkey = uid + "_" + timestamp;

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
        contentTable.put(put);
        //b
        //1、查看用户关系表，得到当前用户的所有粉丝
        Table relationTable = conn.getTable(TableName.valueOf(TABLE_RELATION));
        //2、取出粉丝信息
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));

        Result result = relationTable.get(get);
        //将粉丝这个uid遍历保存到list集合中
        List<byte[]> fans = new ArrayList<>();

        //遍历取出所有粉丝
        for (Cell cell : result.rawCells()) {
            fans.add(CellUtil.cloneQualifier(cell));
        }

        //如果没有粉丝，则结束
        if(fans.size() <= 0) return;

        //2、开始向粉丝收件箱表中添加数据
        Table inboxTable = conn.getTable(TableName.valueOf(TABLE_INBOX));
        //每一个粉丝，都要向收件箱表中添加内容，所以，每个粉丝都是一个不同Put对象
        List<Put> puts = new ArrayList<>();
        for(byte[] fan : fans){
            Put fansPut = new Put(fan);
            fansPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowkey));
            puts.add(fansPut);
        }
        inboxTable.put(puts);

        inboxTable.close();
        relationTable.close();
        contentTable.close();
        conn.close();
    }

    /**
     * 用户关注逻辑
     * a、在微博用户关系表中，对当前主动操作的用户中添加新的关注的好友
     * b、在微博用户关系表中，对被关注用户添加粉丝（当前操作的用户）
     * c、当前操作用户的微博收件箱表添加所关注的人发布的微博rowkey
     */
    public void addAttends(String uid, String... attends) throws IOException {
        //参数过滤
        if(attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) return;
        Connection conn = ConnectionFactory.createConnection(conf);

        //a、b
        Table relationTable = conn.getTable(TableName.valueOf(TABLE_RELATION));
        List<Put> puts = new ArrayList<>();
        Put attendPut = new Put(Bytes.toBytes(uid));
        for(String attend : attends){
            //为当前用户（我）添加关注的人
            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
            //为被我关注的人，添加粉丝
            Put fansPut = new Put(Bytes.toBytes(attend));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        }
        puts.add(attendPut);
        relationTable.put(puts);

        //c
        Table contentTable = conn.getTable(TableName.valueOf(TABLE_CONTENT));
        Scan scan = new Scan();

        //用于存放取出来的关注的人的微博rowkey
        List<byte[]> rowkeys = new ArrayList<>();

        for(String attend : attends){
            //根据关注的人的uid，过滤微博rowkey，通过前置位匹配过滤
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(filter);
            //过滤扫描全表
            ResultScanner result = contentTable.getScanner(scan);
            //迭代遍历
            Iterator<Result> iterator = result.iterator();
            while (iterator.hasNext()) {
                Result r = iterator.next();
//                for (Cell cell : r.rawCells()) {
//                    rowkeys.add(CellUtil.cloneRow(cell));
//                }
                rowkeys.add(r.getRow());
            }
        }
        //将取出的微博rowkey放置于当前操作的用户的收件箱表中
        //如果关注的这几个人，都没有发布过微博，则直接返回
        if(rowkeys.size() <= 0) return;

        //得到微博收件箱表
        Table inboxTable = conn.getTable(TableName.valueOf(TABLE_INBOX));
        //用于存放多个关注的用户的发布多条微博的rowkey
        Put put = new Put(Bytes.toBytes(uid));
        for(byte[] rk : rowkeys){
            String rowkey = Bytes.toString(rk);
            //截取被你关注的那个人的UID，从微博rowkey中截取
            String attendUID = rowkey.substring(0, rowkey.indexOf("_"));
            //取出微博当时发布的时间的时间戳，作为收件箱表中的版本号
//            long timestamp = Long.parseLong(rowkey.substring(rowkey.indexOf("_") + 1));
            //将微博rowkey添加到指定单元格中
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUID), rk);
        }
        inboxTable.put(put);
        inboxTable.close();
        contentTable.close();
        relationTable.close();
        conn.close();
    }

    /**
     * 用户取关逻辑
     * a、在微博关系表中，对当前操作的用户删除他所关注的人的uid
     * b、在微博关系表中，对被取消关注的人，删除粉丝（我）
     * c、在微博收件箱表中，删除取关的人发布的微博rowkey
     */
    public void removeAttends(String uid, String... attends) throws IOException {
        //参数过滤
        if(attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) return;
        Connection conn = ConnectionFactory.createConnection(conf);

        //a、b
        Table relationTable = conn.getTable(TableName.valueOf(TABLE_RELATION));
        //所有待删除的Delete对象
        List<Delete> deleteList = new ArrayList<>();
        Delete attendDelete = new Delete(Bytes.toBytes(uid));
        for(String attend : attends){
            attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
            Delete delete = new Delete(Bytes.toBytes(attend));
            delete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            deleteList.add(delete);
        }
        deleteList.add(attendDelete);
        relationTable.delete(deleteList);

        //c
        Table inboxTable = conn.getTable(TableName.valueOf(TABLE_INBOX));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for(String attend : attends){
            inboxDelete.addColumns(Bytes.toBytes("info"),Bytes.toBytes(attend));
        }

        inboxTable.delete(inboxDelete);
        inboxTable.close();
        relationTable.close();
        conn.close();
    }

    /**
     * 获取微博实际内容
     * a、获取操作用户的收件箱表中的微博rowkey
     * b、根据得到的rowkey，去微博内容表中获取内容
     * c、将得到的内容序列化为json或者封装到一个JavaBean中
     */

    public List<Message> getAttendsContent(String uid, int versions) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table inboxTable = conn.getTable(TableName.valueOf(TABLE_INBOX));
        //a
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(versions);

        List<byte[]> rowkeys = new ArrayList<>();
        Result result = inboxTable.get(get);
        for (Cell cell : result.rawCells()) {
            rowkeys.add(CellUtil.cloneValue(cell));
        }
        //b
        Table conentTable = conn.getTable(TableName.valueOf(TABLE_CONTENT));
        //用于存放所有的微博内容的get
        List<Get> gets = new ArrayList<>();
        for(byte[] rk : rowkeys){
            Get g = new Get(rk);
            gets.add(g);
        }
        Result[] results = conentTable.get(gets);
        //c
        //将每一条微博内容封装为一个Message对象
        List<Message> messages = new ArrayList<>();
        for(Result res : results){
            for(Cell cell : res.rawCells()){
                String rowkey = Bytes.toString(res.getRow());
                String userid = rowkey.split("_")[0];
                String ts = rowkey.split("_")[1];
                String content = Bytes.toString(CellUtil.cloneValue(cell));
                messages.add(new Message(userid, ts, rowkey, content));
            }
        }
        conentTable.close();
        inboxTable.close();
        conn.close();
        return messages;
    }

    /**
     * 封装测试
     */

    private void testPublishContent(Weibo weibo) throws IOException {
        weibo.publishContent("1001", "小姐姐小姐姐，网恋吗，我正太音");
        weibo.publishContent("1002", "小姐姐小姐姐，喝茶吗，我铁观音");
        weibo.publishContent("1003", "小姐姐小姐姐，上天吗，我窜天猴");
        weibo.publishContent("1001", "小姐姐小姐姐。");
    }

    private void testAddAttend(Weibo weibo) throws IOException {
        weibo.addAttends("1002", "1001");
    }

    private void testRemoveAttend(Weibo weibo) throws IOException {
        weibo.removeAttends("1002", "1001", "1003", "1004");
    }

    private void testShowMessage(Weibo weibo) throws IOException {
        List<Message> messages = weibo.getAttendsContent("1002", 5);
        System.out.println(messages);
    }

    public static void main(String[] args) throws IOException {
        Weibo weibo = new Weibo();
        weibo.init();
//        weibo.testPublishContent(weibo);
//        weibo.testAddAttend(weibo);
//        weibo.testRemoveAttend(weibo);
//        weibo.testShowMessage(weibo);
    }
}