# Afflunce
#说明：
##第一步：基础数据准备
消费能力指数的聚类第一版本有如下几个维度组成
###1. 来自CRM的，使用attrCRM2程序，将月度的CRM数据取若干个维度，并获取手机设备的价格。手机价格通过imei_tac关联获得设备型号，通过网络爬取加工后的设备型号--价格码表，得到每个用户使用设备的价格
###2. 来自电信行为的，使用attrUni程序，将日模块的短信、用户通话按照月度聚合统计
###3. 来自出行信息表的，使用commuteDistance程序，将一个月的数据聚合，把符合条件的journey筛选出来，获取频次最高的作为通勤路线，并计算通勤距离
###4. 来自poi的，使用poiMetrics程序，将一个月的数据聚合，把用户在本地和外地的poi数量，驻留频次统计出来
###5.来自标签的，使用atrNumTags程序，统计每个用户的互联网标签数量（下一步的重点工作之一，分析不同标签的价值）
###6.来自房价的，不在本程序中，使用网络爬虫爬取小区房价信息，使用地图插值算法（Arcgis等软件），将用户的home POI与其相关联，得到房价信息（下一步获取多个房源信息，提高密度，剔除异常值）

##第二步：获取均值
是固体不过getAverage程序，将多个月度的数据求取均值（也可以考虑更好的方法，剔除异常的影响）

##第三步：剔除异常值
使用dataSmoother程序，将离群值用上下限替代（使用四分位数的概念，使用UOF和LOF剔除；如果更严格一些，使用UAV和LIF剔除所有可疑离群值）

##第四步：使用聚类程序进行聚类
使用SpendingPower2程序，根据具体数据质量情况选择自己需要的维度进行聚类；
注意如果使用PCA，需要研究贡献度
如果不使用PCA，则使用SpendingPowerWithoutPCA程序
