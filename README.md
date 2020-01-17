This repository contains reference solutions and utility classes for the Flink Training exercises 
on [http://training.data-artisans.com](http://training.data-artisans.com).
# myflink


平均响应时间	
字段：cost（单位毫秒）

时间窗口：每分钟

计算公式：

1. 过滤cost<0和cost>30,000ms的请求

2. sum（所有apigw应用实例的所有接口cost字段值）/总请求次数

字段：cost（单位毫秒）

时间窗口：每分钟

计算公式：

1. 过滤cost<0和cost>30,000ms的请求

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
