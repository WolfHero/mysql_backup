## MySQL数据库每日滚动备份到阿里云OSS一体化脚本

基于Python语言，通过mysqldump和阿里云oss库，设置好定时任务，可以每天把数据库全量备份到OSS，gzip压缩并且滚动删除旧备份，保持硬盘空间占用低。

目前还比较简陋，一次只能备份一个数据库，只能全量备份，只支持阿里云OSS，小项目可以凑合用。

### 使用方法

#### 安装依赖库

开发环境为Python 3.9，理论上没有版本兼容性问题

```bash
pip install oss2 python-dotenv
```

#### 创建一个MySQL用户单独用来备份数据库

localhost为仅允许本地访问，创建用户后仅授权最小权限

```sql
CREATE USER 'your_username'@'localhost' IDENTIFIED BY 'your_password';
GRANT SELECT, LOCK TABLES ON your_schema.* TO 'your_username'@'localhost';
GRANT FLUSH_TABLES ON *.* TO 'your_username'@'localhost';
FLUSH PRIVILEGES;
```

#### 编辑.env中的环境变量

包括数据库连接配置，OSS连接配置，mysqldump可执行文件路径等

#### 配置定时任务

可以先执行一下测试效果再配置，以下是Linux的定时任务配置方法，Windows可以通过计算机管理→系统工具→任务计划程序配置

```bash
# 编辑cron任务
crontab -e

# 每天凌晨2点执行
0 2 * * * /usr/bin/python3 /path/to/main.py
```
#### 关于mysqldump可执行文件

mysqldump可执行文件可以从MySQL服务端、MySQL Workbench、MySQL Docker容器等位置提取到，由于版权问题，项目中不予附带