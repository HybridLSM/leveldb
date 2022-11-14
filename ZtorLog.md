# 面向SSD-HDD混合存储的LSM-Tree优化

## Task List

### DONE
- 完成路径分离，简单将L0和L1放入SSD，其余放入HDD
- 添加热数据记录表、更新表和旧数据积分表
- 添加上述三个表的数据记录逻辑

### DOING

### TODO
- L1、L2层的冷热划分
- Compaction输入文件的选取
- Compaction过程使用HDD
- Compaction过程的旧数据清楚和冷热数据分离
- Get过程的查询修改