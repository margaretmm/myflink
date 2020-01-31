This repository contains reference solutions and utility classes for the Flink Training exercises 
on [http://training.data-artisans.com](http://training.data-artisans.com).
# myflink


https://www.cnblogs.com/029zz010buct/p/9774295.html

k8s operator:
https://blog.gmem.cc/crd
https://blog.csdn.net/weixin_41806245/article/details/94451734


sidecar :
https://segmentfault.com/a/1190000021394687?utm_source=tag-newest

深入剖析 Kubernetes MutatingAdmissionWebhook:https://blog.hdls.me/15564491070483.html


https://mp.weixin.qq.com/s?__biz=MzUzMzU5Mjc1Nw==&mid=2247485186&idx=1&sn=0765fa3e47aa9caae5c5068bd916f913&scene=21#wechat_redirect

https://mp.weixin.qq.com/s?__biz=MzIwNDIzODExOA%3D%3D&chksm=8ec1c4aab9b64dbc088d439305c15097c3294f5f6959188cfb7970af3a42f3c87ac5aa09df2c&idx=1&mid=2650167523&scene=21&sn=3939067e4f34c78d1e180b012e8a88f5

--------------需求总表
1	监控覆盖需求	提高监控覆盖率，目标：apigw入口核心产品线0,1,2,3级故障监控无遗漏
apigw入口核心产品线场景接口监控全覆盖	  基于静态/动态基线加配置阈值的方式监控apigw入口核心业务场景接口指标（主要包括四个黄金指标平均响应时间，错误率，每分钟错误数和吞吐率）  高

2	故障快速分析定位需求	
降低故障MTTR，目标：apigw入口核心产品线0,1,2级故障分析定位<10min
apigw入口核心产品线场景故障快速分析定位需求	基于全链路Trace+监控metrics指标实现应用拓扑分析	中

3	可用性度量	推进研发关注请求级别的微观可用性数据	apigw入口核心产品线微观可用性度量	
微观可用性=1-（请求失败次数+超过N响应时间的请求次数）/总请求次数   统计最小粒度（天）N值针对每个接口可配   高

4	横向需求	基于大数据实时+ETL规则灵活配置	 


----------------指标：
延迟　　	
平均响应时间	从应用接收到请求到响应发送成功的时长
平均响应时间=AVERAGE（所有请求响应时长）	毫秒（ms）
最大响应时间	最大响应时间=MAX（所有请求响应时长）	毫秒（ms）
最小响应时间	最小响应时间=MIN（所有请求响应时长）	毫秒（ms）
较慢请求数	较慢请求可自定义配置规则为 >N
较慢请求数=SUM（响应时长>N的请求）	次
较慢请求比率	较慢请求比率=响应时长>N的请求/总请求数	%
很慢请求数	很慢请求可自定义配置规则为 >N
很慢请求数=SUM（响应时长>N的请求）	次
很慢请求比率	很慢请求比率=响应时长>N的请求/总请求数	%
停滞请求数	停滞请求可自定义配置规则为 >N
停滞请求数=SUM（响应时长>N的请求）	次
停滞请求比率	停滞请求比率=响应时长>N的请求/总请求数	%


流量	
吞吐率	吞吐率：每分钟请求总数	次/分钟

错误　
每分钟错误数	每分钟错误数=每分钟错误请求总数	次/分钟
错误率	错误率=错误请求总数/总请求数	%
每分钟http错误数	每分钟错误数=每分钟错误请求总数	次/分钟
http错误率	错误率=错误请求总数/总请求数	%


---------需求1：入口核心产品线场景接口监控全覆盖

平均响应时间	字段：cost（单位毫秒）
时间窗口：每分钟
计算公式：
1. 过滤cost<0和cost>30,000ms的请求
2. sum（所有apigw应用实例的所有接口cost字段值）/总请求次数


字段：cost（单位毫秒）
时间窗口：每分钟
计算公式：1. 过滤cost<0和cost>30,000ms的请求
2. groupby：productId,method,uri字段
3. sum（所有apigw应用实例的所有接口cost字段值）/总请求次数



吞吐率	
时间窗口：每分钟
计算公式：
1. 吞吐率（单位次/分钟）=sum（所有apigw应用实例的所有接口请求）

时间窗口：每分钟
计算公式：
1. groupby：productId,method,uri字段
2. 吞吐率（单位次/分钟）=sum（所有apigw的所有接口请求）

业务错误率	
时间窗口：每分钟
计算公式：
1. 过滤字段bizCode的值非0以及非200的请求
3. 过滤部分bizCode字段值（黑名单机制，可配置）
4. 错误率（%）=sum（过滤字段bizCode的值非0以及非200的请求）/总请求数

时间窗口：每分钟
计算公式：
1. 过滤字段bizCode的值非0以及非200的请求

3. groupby：productId,method,uri字段

4. 过滤部分bizCode字段值（黑名单机制，可按接口维度配置）

5. 错误率（%）=sum（过滤字段bizCode的值非0以及非200的请求）/总请求数

业务每分钟错误数	
时间窗口：每分钟

计算公式：

1. 过滤字段bizCode的值非0以及非200的请求

2. 过滤部分bizCode字段值（黑名单机制，可配置）

3. 每分钟错误数=sum（过滤后bizCode的值非0以及非200的请求）

时间窗口：每分钟

计算公式：

1. 过滤字段bizCode的值非0以及非200的请求

2. groupby：productId,method,uri字段

3. 过滤部分bizCode字段值（黑名单机制，可按接口维度配置）

4. 每分钟错误数=sum（过滤后bizCode的值非0以及非200的请求）

http错误率	
时间窗口：每分钟

计算公式：

1. 过滤字段status的值非5XX的请求

2. 过滤部分status字段值（黑名单机制，可配置）

3. 错误率（%）=sum（过滤后status的值非5XX的请求）/总请求数

时间窗口：每分钟

计算公式：
1. 过滤字段status的值非5XX的请求
2 groupby：productId,method,uri字段
3. 过滤部分status字段值（黑名单机制，可按接口维度配置）
4. 错误率（%）=sum（过滤后status的值非5XX的请求）/总请求数
http每分钟错误数	
