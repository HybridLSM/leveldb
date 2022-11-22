# 面向SSD-HDD混合存储的LSM-Tree优化

## Task List

### DONE
- 完成路径分离，简单将L0和L1放入SSD，其余放入HDD
- 添加热数据记录表、更新表和旧数据积分表
- 添加上述三个表的数据记录逻辑
- L1、L2层的冷热划分
- Compaction输入文件的选取
- Compaction过程的旧数据清楚和冷热数据分离
- Get过程的查询修改

### DOING
- 检查可能的线程安全问题

### TODO
- Compaction过程使用HDD

## 存在的问题和修改方案
- **在Get的过程中，可能会由于同时发生的Compaction（主要是Major Compaction）导致`key_upd`表被更新，而`version`没来得及更新，导致得到错误的文件号，使得查询失败。**

  目前的解决方案是在通过`key_upd`表查询失败时，再调用一次正常的`Get`确保查询成功。之后修改为在Compaction过程中，先记录修改，在`version`更新的同时统一提交`key_upd`的更新。具体修改思路如下：

  1. `key_upd`表添加存储更新记录的变量。
  2. `key_upd`表要添加标识位，用于标志表进入只读状态。
  3. 当表为只读状态时，所有的更新将不会直接修改表内的`cache_`，而是先统一被存储。
  4. 通过`Commit()`一次提交所有的更新，随后解锁；通过`GiveUpUpdate()`放弃并清除所有未提交的更新，随后解锁。  

  其实上面这样做还是存在一定风险，可以考虑表内同时维护两个`cache_`，仅有一个激活对外提供数据，更新过程中只更新未激活的那一个，完成后激活。

- **在Major Compaction过程中，可能会存在因`hot_table`改变而导致前后相同key被判定是否为热数据的结果不一，存在隐患。**
  
  解决方案参考上一条，对`hot_table`做同样的修改，保证其在Major Compaction的过程中不被修改。

- **`filenum_to_level_`的多线程支持。**