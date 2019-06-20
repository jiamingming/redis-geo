#### redis geo

按照经纬度和指定半径(单位:km)查询线上楼宇
参数说明:

- lon  经度
- lat   纬度
- rad  半径

利用redis的geo结构存储楼宇的位置信息

- GEOADD key longitude latitude member [longitude latitude member …]

查询时

- GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [ASC|DESC] [COUNT count]
