# esQueryTool
Base For "github.com/olivere/elastic" ElasticSearch Query Tool

### 前置条件
必须加入数据，否则es查不到indexName会报空指针异常  

### 功能简述
从es中查找指定时间戳区间内的用户间聊天记录  
支持按指定时间区域查找数据  
支持按多个条件排序  
支持分页

### 返回信息
返回查找到的数据和hit的条数和error