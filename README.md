# 什么是Errdb

Errdb(Extensible Round Robin Database)是可扩展的环形的数据库，用于存储储时间序列性能指标数据。

# Errdb架构

Errdb采用内存+文件的方式存储最近一段时间(T0~Tn)之间的指标数据，概念模型：

![Architecture](https://github.com/erylee/errdb/raw/master/design/concept.png "Concept")

Errdb实现上每个Key对应一个Round Robbin的存储文件，时间序列的性能数据先写入
内存，然后定期DUMP到文件。

实现模型如下：

![Architecture](https://github.com/erylee/errdb/raw/master/design/arch.png "Architecture")

# Errdb应用

Errdb在WLAN网管用于存储原始的性能数据，一般存储最近两天，WLAN网管界面上的所有实时性能图表，都来自ERRDB。

Errdb生成的Journal文件在WLAN网管中，会通过定时任务导入到Oracle进行聚合归并生成报表数据。

# Errdb启动停止

1. ./bin/errdb start 启动Errdb进程
2. ./bin/errdb_ctl status 查询Errdb状态
3. ./bin/errdb stop 停止Errdb进程

# Errdb编译

Errdb采用rebar编译
```
mkdir -p var var/data

rebar3 dialyzer

rebar3 compile (编译)
rebar3 clean (清除)
rebar3 as product release (发布)
rebar3 as prod tar

rebar3 clean && rebar3 compile && erl -boot start_sasl -pa _build/default/lib/*/ebin -config etc/errdb.config

observer:start().
application:start(errdb).
```

# Errdb文件存储

Errdb缺省情况下把数据文件存储在var/rrdb/目录, journal文件存储在var/journal/目录。

通过etc/errdb.config配置文件，可重设相关存储目录。

# Errdb设计参考

+ [Redis]: http://redis.io
+ [RRDTOOL]: http://oss.oetiker.ch/rrdtool/
+ [Cassandra]: http://cassandra.apache.org/
+ [Mongodb]: http://www.mongodb.org/

