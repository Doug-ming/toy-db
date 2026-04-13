# toy-db

toy-db是一款基于 Java 开发的实验型嵌入式磁盘 B+ 树存储引擎，专注实现数据的可靠持久化与精确查询。项目采用手动设计的磁盘页结构、以slotted page技术高效存储变长记录，数据文件以索引组织表实现。完整还原数据库存储核心原理。无冗余框架、无工业级特性，仅保留最本质的存储逻辑，适合学习、实验与教学场景。

<!-- TOC -->
* [toy-db](#toy-db)
  * [编写初衷](#编写初衷)
  * [难点](#难点)
  * [支持的操作：insert & search](#支持的操作insert--search)
  * [内置数据类型](#内置数据类型)
    * [BOOL](#bool)
    * [INT32](#int32)
    * [INT64](#int64)
    * [CHAR](#char)
    * [VARCHAR](#varchar)
    * [DATE](#date)
    * [DATETIME](#datetime)
  * [设计](#设计)
    * [索引组织表](#索引组织表)
    * [slotted page技术](#slotted-page技术)
  * [未实现的工业级数据库特性](#未实现的工业级数据库特性)
    * [并发控制](#并发控制)
    * [ACID事务](#acid事务)
    * [预写日志与延迟刷盘](#预写日志与延迟刷盘)
    * [LSN与检查点](#lsn与检查点)
    * [删除与垃圾回收](#删除与垃圾回收)
    * [页分裂时多个磁盘页刷盘的原子性担保](#页分裂时多个磁盘页刷盘的原子性担保)
    * [数据页损坏后的恢复](#数据页损坏后的恢复)
    * [任何类似SQL的DSL，需通过Java api操作](#任何类似sql的dsl需通过java-api操作)
  * [使用场景](#使用场景)
  * [快速上手](#快速上手)
    * [环境准备](#环境准备)
    * [构建](#构建)
    * [运行](#运行)
<!-- TOC -->

## 编写初衷

为深入理解B+树在磁盘存储中的真实应用以及组织一棵B+树的细节问题，在实现内存版B树后发现其价值有限。B+树本身就是基于在磁盘上建立有序索引的动机发明的，因此决定将内存中的B树落实到磁盘，即从零开始在磁盘上组织一棵B+树且专注于数据的持久化存储与正确取回。

## 难点

访问磁盘和访问主存有所不同。通常的Java应用都在jvm提供的一套虚拟内存机制之上编程，用户只管new对象，不知道这个对象位于进程虚拟内存地址空间中的什么位置，也不关心对象内数据的布局，对象的寻址方式也是透明的。而访问磁盘本质是通过操作系统调用，读写磁盘时都需要指定目标文件内的偏移量。每个磁盘页的数据布局需要自行设计，且需要手动管理磁盘页的寻址。还涉及同一份数据在适合内存的形式与适合磁盘的形式之间的互相转换，即序列化和解序列化。

## 支持的操作：insert & search
因实验型的第一版只专注存的进去，取得回来。因此只实现insert和search操作。insert表示插入一行记录，search表示按key搜索一行记录。

## 内置数据类型
### BOOL
bool类型，在磁盘上用单个字节表示，0x01表示true，0x00表示false。

### INT32
32位整型的二进制补码，对应java中的int类型，在磁盘上用4个字节表示，字节序为大端序（高位字节位于低地址）。

### INT64
64位整型的二进制补码，对应java中的long类型，在磁盘上用8个字节表示，字节序为大端序（高位字节位于低地址）。

### CHAR
所有数据库中都很常见的定长文本类型，用户可指定其最大长度。在磁盘上的表示为UTF-8字符集编码后的字节序列，若编码后的字节序列未达到最大长度，用空格填充。

### VARCHAR
所有数据库中都很常见的变长文本类型，用户可指定其最大长度。在磁盘上的表示为UTF-8字符集编码后的字节序列。

### DATE
日期类型，只包含年月日信息。在磁盘上用4个字节存储，分别存世纪，年，月，日。

### DATETIME
日期时间类型，包含年月日时分秒信息。在磁盘上用7个字节存储，分别存世纪，年，月，日，时，分，秒。

## 设计
### 索引组织表
数据文件采用索引组织表（Index Organized Table, IOT）实现，数据记录直接存储在B+树索引结构中，按逻辑主键有序组织。不使用独立于数据文件外的额外索引文件记录行所在的物理位置，逻辑主键值直接决定记录在磁盘文件中的物理位置，实现索引与数据一体化存储。

### slotted page技术
所有磁盘页均采用slotted page（槽页）技术，用于高效存储变长记录。每个页面内部划分为槽目录区与实际数据区，槽目录项记录每条记录的偏移与长度，记录逻辑顺序由槽目录项的顺序决定，与物理偏移量无关，天然支持变长数据高效管理。第一版暂不实现槽目录稀疏化，保持结构简洁易懂。

## 未实现的工业级数据库特性

以下特性为工业级数据库地核心能力，本实验版本暂不实现，后续可考虑迭代。

### 并发控制

### ACID事务

### 预写日志与延迟刷盘

### LSN与检查点

### 删除与垃圾回收

### 页分裂时多个磁盘页刷盘的原子性担保

### 数据页损坏后的恢复

### 任何类似SQL的DSL，需通过Java api操作

## 使用场景
适用于单线程、离线写入、运行时只读的实验场景：先离线批量将数据写入磁盘文件，后续仅提供基于主键的精确查询，不支持并发、更新、删除与复杂查询。仅用于存储引擎原理学习、教学演示、B+树磁盘实现验证，不可用于生产环境。

## 快速上手
### 环境准备
1.JDK 11+
2.以MAVEN作为构建工具

### 构建
git clone https://github.com/toy-db/toy-db.git

cd toy-db

maven clean install

### 运行

1.创建数据库对象并初始化

/* 直接调用com.jd.gaoming.storage.engine.ToyDB类的构造器，传入的字符串参数表示数据目录，即存放数据文件的目录 */
ToyDB db = new ToyDB("D:/DataFile");
db.open();

2.建表
假设一张表的表结构为：
employee_id int4       -- 员工ID
tax_number char(4)     -- 税号 
hire_date  date 4字节   -- 入职日期
is_male 1字节 bool      -- 性别
first_name varchar(32) -- first name
last_name varchar(32)  -- last name

/* 调用静态方法com.jd.gaoming.storage.engine.table.ITableSchema :: createTable构建表结构对象
   第一个整型参数表示主键列在TableSchema中的位置
   第二个参数为表示列的IColumn可变参数列表
   调用静态方法com.jd.gaoming.storage.engine.table.IColumn :: createColumn创建表示列的对象
   第一个参数表示该列在TableSchema中的位置
   第二个参数表示列名
   第三个参数为列的最大长度
   第四个参数为数据类型，通过com.jd.gaoming.storage.engine.table.datatypes.DataTypeRegistry的静态常量获取
*/
final ITableSchema tableSchema = ITableSchema.createTableSchema(0,
IColumn.createColumn(0,"employee_id",null, DataTypeRegistry.INT_32),
IColumn.createColumn(1,"tax_number",4,DataTypeRegistry.CHAR),
IColumn.createColumn(2,"hire_date",null,DataTypeRegistry.DATE),
IColumn.createColumn(3,"is_male",null,DataTypeRegistry.BOOL),
IColumn.createColumn(4,"first_name",255,DataTypeRegistry.VARCHAR),
IColumn.createColumn(5,"last_name",255,DataTypeRegistry.VARCHAR));

/*
   调用ToyDB::createTable建表，此调用后会在数据文件目录中创建employee.db数据文件
*/
ITable table = db.createTable("employee",tableSchema);

3.插入数据
/*
  先通过调用静态方法com.jd.gaoming.storage.engine.table.IRow :: createEmptyRow传入TableSchema构建一个空行
  再调用IRow :: setValue将字段值写入
  调用Table :: insert传入刚构建的IRow对象就能将该行记录持久化地写入employee.db文件中
*/
IRow row_1 = IRow.createEmptyRow(tableSchema);
row_1.setValue(0,1);
row_1.setValue(1,"abcd");
row_1.setValue(2,new java.util.Date());
row_1.setValue(3,true);
row_1.setValue(4,"first name");
row_1.setValue(5,"last name");
table.insert(row_1);

4.取回数据
/*
   用相同的数据文件目录创建ToyDB对象并初始化
*/
ToyDB db = new ToyDB("D:/DataFile");
db.open();

/*
   调用ToyDB :: from传入表明即可获得Table对象
   调用ITable :: search传入主键值查询
*/
ITable table = db.from("employee");
IRow row = table.search(1);

