# -*- coding: utf-8 -*-

import pymysql
import pandas.io.sql as sql
import pandas as pd
import numpy as np
import pynlpir

#计算分词后机构名的相似度(df1和df2是以机构名为索引，只含有一列的DataFrame，
#这一列值全为1）
def sim(df1,df2):
    df3=df1.join(df2,how='outer')
    df3=df3.fillna(0) #用0填充nan值
    numerator=0
    denominator1=0
    denominator2=0
    for i in range(0,df3.iloc[:,0].size):
        numerator+=df3.iloc[i,0]*df3.iloc[i,1] #余弦相似度的分子
        denominator1+=np.square(df3.iloc[i,0])
        denominator2+=np.square(df3.iloc[i,1])
    denominator=np.sqrt(denominator1)*np.sqrt(denominator2) #余弦相似度的分母
    cos=numerator/denominator
    return cos

#将mysql数据库author_diff中的表jg_sim和all_zzjg_1导入python的dataframe结构
conn=pymysql.connect(host='127.0.0.1',port=3306,user='maoyue',passwd='maobuyi',db='author_diff',charset='utf8')
df_sim=sql.read_sql_query("select * from jg_sim_pm",conn)
df_all=sql.read_sql_query("select * from all_zzjg_1",conn)
conn.commit()
conn.close()

pynlpir.open()

#遍历jg_sim的所有同名作者+机构
for i in range(0,df_sim.iloc[:,0].size):
#每条记录先清空使用到的列表
    list_pm1=[]
    list_pm2=[]
    words1=[]
    words2=[]
    #查询all_zzjg_1中作者+机构 实体写的文章篇名（pm）并存储在list_pm1和list_pm2
    for j in range(0,df_all.iloc[:,0].size):
        if(df_all.loc[j,'ZZMC']==df_sim.loc[i,'ZZMC']):
            if(df_all.loc[j,'JGMC']==df_sim.loc[i,'JG1']):
               list_pm1.append(df_all.loc[j,'PM'])
            else if(df_all.loc[j,'JGMC']==df_sim.loc[i,'JG2']):
                list_pm2.append(df_all.loc[j,'PM'])
        else:
            continue
#对两个篇名的list遍历并分词，分词结果储存在words1和words2两个list里
    for k in list_pm1:
        words1.extend(pynlpir.segment(k,pos_tagging=False))
    for l in list_pm2:
        words2.extend(pynlpir.segment(l,pos_tagging=False))

    words1=list(set(words1))#词辞典排序 二分法搜索
    words2=list(set(words2))
    value1=[1.0 for m in range(0,len(words1))]
    value2=[1.0 for m in range(0,len(words2))]
    df1=pd.DataFrame(value1,index=words1,columns=['value1'])
    df1=pd.DataFrame(value2,index=words2,columns=['value2'])
    df_sim.loc[i,'SIM']=sim(df1,df2)

pynlpir.close()


#将dataframe中的内容写入数据库，覆盖掉原来的jg_sim_pm表
from sqlalchemy import create_engine
conn=create_engine('mysql+pymysql://author_diff:maoyue@localhost:3306/author_diff?charset=utf8')
sql.to_sql(df_sim,'jg_sim_pm',conn,schema='author_diff',if_exists='replace',index=False)


