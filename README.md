1、创建城市表
create table city_info(city_id int,city_name string,area string)
row format delimited fields terminated by '\t';

2、导入城市数据
sqoop import \
--connect jdbc:mysql://localhost:3306/business \
--username root --password root \
--table city_info -m 1 \
--mapreduce-job-name city_info_imp \
--delete-target-dir \
--hive-table test.city_info \
--hive-import \
--fields-terminated-by '\t' \
--hive-overwrite;

3、创建产品表
create table product_info(product_id int,product_name string,extend_info string)
row format delimited fields terminated by '\t';

4、导入产品数据
sqoop import \
--connect jdbc:mysql://localhost:3306/business \
--username root --password root \
--table product_info -m 1 \
--mapreduce-job-name product_info_imp \
--delete-target-dir \
--hive-table test.product_info \
--hive-import \
--fields-terminated-by '\t' \
--hive-overwrite;

5、创建用户点击表
create table user_click(user_id int,session_id string,action_time string,city_id int,product_id int)
partitioned by (create_day string)
row format delimited fields terminated by ',';
load data local inpath '/home/hadoop/data/user_click.txt' into table user_click partition(create_day='2016-05-05');

6、创建计算结果表
create table top3(product_id int,product_name string,area string,click_count int,rank int,day1 string)
partitioned by (day string)
row format delimited fields terminated by '\t';

7、临时版：数据只有一天，所以没有时间过滤
insert overwrite table top3 partition(day)
select * from (
select t.product_id,t.product_name,t.area,t.click_count,
row_number() over(partition by area order by click_count desc) rank,t.day day1,t.day
from (select u.product_id,p.product_name,c.area,count(1) click_count,date(action_time) day
from user_click u left join product_info p on u.product_id=p.product_id 
left join city_info c on c.city_id=u.city_id group by u.product_id,p.product_name,c.area,date(action_time)) t where t.area is not null) tt where tt.rank<4

7、正式版：每天执行一次，计算前一天的top3并写到top3表中
insert overwrite table test.top3 partition(day)
select * from (
select t.product_id,t.product_name,t.area,t.click_count,
row_number() over(partition by area order by click_count desc) rank,t.day day1,t.day
from (select u.product_id,p.product_name,c.area,count(1) click_count,date(action_time) day
from (select * from test.user_click where date(action_time)=date(date_sub(current_date,1))) u left join test.product_info p on u.product_id=p.product_id 
left join test.city_info c on c.city_id=u.city_id group by u.product_id,p.product_name,c.area,date(action_time)) t where t.area is not null) tt where tt.rank<4

8、导出top3到mysql
sqoop export \
--connect jdbc:mysql://localhost:3306/business \
--username root --password root \
--table top3 -m 1 \
--mapreduce-job-name top3_imp \
--export-dir /user/hive/warehouse/test.db/top3/day=2016-05-05 \
--columns "product_id,product_name,area,click_count,rank,day" \
--fields-terminated-by '\t' 

9、调度执行，每天凌晨1点执行计算前一天的数据并导出到mysql
crontab -e * 1 * * * /home/hadoop/data/test.sh

test.sh：
#!/bin/bash
yesterday=`date -d last-day +%Y-%m-%d`  
echo $yesterday
echo "hive begin........."
hive -e "set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table test.top3 partition(day)
select * from (
select t.product_id,t.product_name,t.area,t.click_count,
row_number() over(partition by area order by click_count desc) rank,t.day day1,t.day
from (select u.product_id,p.product_name,c.area,count(1) click_count,date(action_time) day
from (select * from test.user_click where date(action_time)=date(date_sub(current_date,1))) u left join test.product_info p on u.product_id=p.product_id 
left join test.city_info c on c.city_id=u.city_id group by u.product_id,p.product_name,c.area,date(action_time)) t where t.area is not null) tt where tt.rank<4;"
echo "hive end."
echo "sqoop export begin......"
sqoop export \
--connect jdbc:mysql://localhost:3306/business \
--username root --password root \
--table top3 -m 1 \
--mapreduce-job-name top3_imp \
--export-dir /user/hive/warehouse/test.db/top3/day=$yesterday \
--columns "product_id,product_name,area,click_count,rank,day" \
--fields-terminated-by '\t';
echo "sqoop export end."
