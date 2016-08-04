# VIPANALYZER 项目介绍


##目前平台
> * 雪球    
> * 中金博客  
> * 摩尔金融
> * 淘股吧
> * 微博 

## Kafka ** Topic ** 说明
> 1、  `self_media`  

> Spark 从服务端接收这个topic的消息,根据消息获取表名 和rowkey,spark 从hbase表里取数据进行解析。所有平台都是从这个topic获取

> 2、 `newsparser_news`   

> Spark 解析出数据后，比如新闻链接，标题或者时间，再把这个消息发给服务端。这个消息对四个平台：中金博客，摩尔金融，淘股吧，微博适用。

> 3、 `newsparser_duration`

> Spark 解析出数据后，把消息发送给服务端，这个消息适用平台只有雪球。

####流程图
```flow
st=>start: Start
op=>operation: 服务端通过self_media 发送给spark端
opreceive=>operation: spark端接收之后进行解析，然后发送
cond=>condition: 雪球 yes or no 
m=>operation: 其它四个平台通过newsparser_news发送
opduration=>operation: 通过newsparser_duration发送
e=>end

st->op->opreceive->cond
cond(yes)->opduration->e
cond(no)->m->e
```
---------------------------------
##各平台解析发送消息格式

###公共参数
> | 字段      |  类型   |  备注  |
| --------  | :-----: | :---- |
| id   | int64  |  时间戳（发送消息时的实时时间毫秒） |
| islogin    | int8   | 是否需要登录（一般需要登录给1，服务爬虫端爬取）    |
| attrid   | string  |  平台号 |
| depth   | int8  | 2 |
| cur_depth    | int8   |  2 |
| method   | string  |  判断网页链接是Get还是Post请求，如果是post值为1，Get 值为2 |
| url    | string   |  spark解析获取的新闻url  |

### 各个平台解析详情介绍
> | 平台中文名称      |  平台号   |  Spark解析获取插入数据库的字段  |备注| 爬取方式|
| --------  | :-----: | :---- |:---- |:---- |
| 雪球   | 40001  | 用户id,新闻标题名，转发数量，回复数量，新闻链接，新闻发布时间戳 | 数据写入数据库，写入成功则下一步发送消息， |逐个爬取大V账号|
| 中金博客   | 40002  | 用户id,新闻标题名，点赞数量，转发数量，评论数量，新闻链接，新闻发布时间戳| 数据写入数据库，写入成功则下一步发送消息 | 关注爬取|
| 摩尔金融   | 40003  | 用户id,新闻标题名，阅读数量，购买数量，文章价格，新闻链接，新闻发布时间戳| 数据写入数据库，写入成功则下一步发送消息  |逐个爬取大V账号|
| 淘股吧   | 40004|用户id,新闻标题，新闻链接，时间戳 | 此平台由于新闻比较特殊，进行特殊处理，spark解析完毕不再发送消息给服务端，直接把数据插入两个数据表，其他平台spark端只操作一个数据表 |关注爬取|
| 微博   | 40005  |用户id,新闻标题名，转发数量，点赞，评论数量，新闻链接，新闻发布时间戳 |数据写入数据库，写入成功则下一步发送消息  |关注爬取|


##Contributor 
>* NiuJiaojiao