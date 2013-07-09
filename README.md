<h1>solr-DIH<h1>
<p> by okwangxing</p>

欢迎使用CPU性能检测工具

目前相对简陋，请下载后导入项目直接使用，后面我们会持续优化。
文档会慢慢补上。

Feature
========
* 支持分库分表
* 支持分布分表步进设置
* 多次多schema

settings
========
```
  <dataConfig>
    <dataSource name="entity_%d" driver="com.mysql.jdbc.Driver" 
    url="jdbc:mysql://192.168.1.1:3306/entity_%d?useUnicode=true&amp;characterEncoding=utf8" 
    user="user" password="pass" shard="true" shardStep="1" shardBegin="0" shardEnd="1"/>
    <document name="entity">
        <entity datasource="entity_%d" name="entity_%d" 
        query="select * from t_entity_%d" 
        deltaImportQuery="select * from t_entity_%d where id='${dih.delta.id}'"  
        deltaQuery="select id from t_entity_%d where ftime &gt; '${dih.last_index_time}'" 
        shard="true" shardStep="1" shardBegin="0" shardEnd="1">
            <field column="id" name="id" />
           ... ...
        </entity>
       </document>
   </dataConfig>
```
