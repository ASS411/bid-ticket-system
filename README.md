# 必得网 —— 仿大麦高并发抢票系统

面向高并发场景设计的分布式微服务票务系统，完整实现了从节目浏览、选座下单、支付回调到订单管理、延迟关单的业务闭环，并针对**抢票秒杀场景**下的高并发、高吞吐、海量数据存储等真实生产问题给出了落地解决方案。

## 项目背景

传统「增删改查」型业务项目在应对热门演唱会门票「秒空」场景时力不从心。本项目以**高并发抢票**为核心难点，还原真实生产环境的微服务架构与中间件体系，重点解决：

- 缓存**穿透 / 击穿 / 雪崩**的完整落地
- 库存扣减的**原子性与不超卖**
- 高并发下的**订单一致性、幂等与分布式事务**
- 海量订单数据的**分库分表**与多维度查询收敛
- 本地缓存与 Redis 的**一致性**（副本失效广播）

## 核心亮点

- **三级缓存体系**：Caffeine 本地缓存 + Redis + MySQL，基于 Window-TinyLFU 与「距开演时间」动态过期策略，配合布隆过滤器与分布式锁，抵御穿透与雪崩
- **Lua 原子扣库存**：座位状态与票档余票收敛于 Redis Hash，单个 Lua 脚本完成「校验 → 扣减 → 锁座位」，杜绝超卖
- **基因分片法**：ShardingSphere 自定义复合分片算法，将用户路由基因嵌入订单号，使 `order_number` 与 `user_id` 两个查询维度必然落于同库同表，消除跨分片聚合
- **分布式锁框架**：注解 + 切面 + 编程式三层封装，支持可重入 / 公平 / 读写锁，锁粒度精确到业务对象
- **Kafka 异步下单**：抢票请求削峰填谷异步化，配合幂等与延迟队列实现 5 分钟未支付自动关单
- **多版本演进**：订单创建从同步加锁到异步削峰的多版本优化，体现逐步演进的高并发设计思路

## 技术栈

| 技术 | 说明 |
| --- | --- |
| Spring Boot 3.3.0 / Spring Cloud 2023.0.2 / Spring Cloud Alibaba 2023.0.1.0 | 微服务基础框架（JDK 17） |
| Nacos | 服务注册与发现、配置中心 |
| Spring Cloud Gateway | 网关路由、RSA+JWT 鉴权、滑动窗口限流 |
| Redis / Redisson 3.32.0 | 缓存、分布式锁、延迟队列、Stream 消息、布隆过滤器 |
| Caffeine 2.9.3 | 本地缓存（Window-TinyLFU） |
| Kafka | 异步下单、订单消息解耦 |
| Elasticsearch / Logstash / Kibana | 节目搜索与日志收集 |
| ShardingSphere 5.3.2 | 分库分表、绑定表、广播表、数据加密 |
| MyBatis-Plus / MySQL / Druid | ORM 与数据存储 |
| Sentinel | 服务熔断降级 |
| Spring Boot Admin / XXL-Job | 服务监控与分布式定时任务 |
| Vue 3 + Vite | 前端订票应用 |

## 系统架构

### 微服务模块（damai-server）

| 服务 | 职责 |
| --- | --- |
| damai-gateway-service | 网关：路由、RSA+JWT 鉴权、限流 |
| damai-user-service | 用户注册登录、布隆过滤器防穿透、用户信息分库分表 |
| damai-program-service | 节目、场次、座位、票档（三级缓存 + 热点预热） |
| damai-order-service | 订单创建（Kafka 异步）、状态流转、基因分片 |
| damai-pay-service | 支付回调、退款（策略模式多支付渠道） |
| damai-base-data-service / damai-customize-service | 基础数据与自定义配置 |
| damai-admin-service | 后台管理 |

### 组件框架（*-framework）

- `damai-redisson-framework`：分布式锁（`@ServiceLock`）、延迟队列、布隆过滤器、幂等注解（`@RepeatExecuteLimit`）
- `damai-redis-tool-framework`：Redis 封装、Redis Stream 消息广播（本地缓存失效）
- `damai-id-generator-framework`：雪花 ID 生成（时钟回拨处理）
- `damai-elasticsearch-framework`：ES 搜索封装
- `damai-thread-pool-framework`：统一线程池（MDC 透传）
- `damai-spring-cloud-framework`：服务初始化、灰度等通用组件

### 目录结构

```
├── damai-server            # 核心业务服务（7 个微服务）
├── damai-server-client     # 各服务 OpenFeign 客户端
├── damai-common            # 公共工具与常量
├── damai-*-framework       # 组件框架库
├── sql/cloud/              # 数据库脚本（1 建库 + 10 业务脚本）
└── vue3                    # 前端（Vue 3 + Vite）
```

## 快速开始

### 环境依赖

- JDK 17、Maven 3.8+
- MySQL 5.7+、Redis、Kafka、Nacos、Elasticsearch

### 1. 初始化数据库

导入 `sql/cloud/` 下全部脚本（先执行 `1_damai_cloud_create_database.sql` 建库，再按序导入各业务脚本；订单/支付/节目/用户表为分片结构 `_0`、`_1`）。

### 2. 配置中间件

中间件地址与账号密码配置在**各服务的本地配置**中（如 `application-local.yml`），该文件默认不入库，需按本机环境自行创建。测试环境要点：

- Redis 需设置访问密码（如 `123456`）
- Kafka `server.properties` 中 `advertised.listeners` 需指向实际可访问地址（`PLAINTEXT://<host>:9092`），否则客户端会尝试连接 localhost
- Nacos 注册中心地址需与各服务 `application.yml` 的 `server-addr` 一致

### 3. 启动后端服务

按以下顺序启动（依赖关系：先注册中心与中间件，再业务服务，最后网关与后台）：

```
base-data-service → customize-service → pay-service → user-service
→ order-service → program-service → gateway-service → admin-service
```

网关默认端口 `6085`。

### 4. 启动前端

```bash
cd vue3
npm install
npm run dev
```

前端通过 Vite 代理将 `/barley-dev` 前缀的请求转发至后端网关（默认 `127.0.0.1:6085`）。

## 配置说明

- `application.yml`（入库）：通用配置，中间件地址默认为 `127.0.0.1` 的本地环境
- `application-local.yml` / `shardingsphere-*-local.yaml`（不入库）：本地环境配置，含中间件地址、账号密码等，clone 后需自行创建
