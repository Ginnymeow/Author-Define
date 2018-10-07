# -*- coding: utf-8 -*-

"""
Created on Sat May  5 20:55:23 2018

@author: keroulin
"""

import pymysql
import pandas.io.sql as sql
import pandas as pd
import numpy as np
import pynlpir

#将mysql数据库author_diff中的表jg_sim导入python的dataframe结构
conn=pymysql.connect(host='127.0.0.1',port=3306,user='author_diff',passwd='linkerou123',db='author_diff',charset='utf8')
df=sql.read_sql_query("select * from jg_sim",conn)
'''
#连接以后另一种将表jg_sim导入python的dataframe的方法
cursor=conn.cursor()
cursor.execute("select * from jg_sim limit 10")
rows=cursor.fetchall()
df=DataFrame(rows,columns=zip(*cursor.description)[0])
cursor.close()
'''
conn.commit()
conn.close()


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

#将机构分词并存在一个dataframe中，索引为词，column名为value，value均为1
pynlpir.open()

for i in range(0,df.iloc[:,0].size):
    if (df.loc[i,'JG1']=='' or df.loc[i,'JG2']==''):
        #两个机构中有一个机构名为空就让相似度SIM2=0
        df.loc[i,'SIM2']=0.0
##包含关系设置为sim=100
    elif (df.loc[i,'JG1'].find(df.loc[i,'JG2'])>-1):
        df.loc[i,'SIM2']=100.0
    else:
        #创建两个DataFrame df1和df2,把两个机构的分词结果作为DataFrame的索引，
        #设置value列，value值全为1，表示每个词在机构名中出现过，调用sim函数计算
        #形成的两个DataFrame的相似度
        words1=pynlpir.segment(df.loc[i,'JG1'],pos_tagging=False)
        words2=pynlpir.segment(df.loc[i,'JG2'],pos_tagging=False) #使用pos_tagging来关闭词性标注
        value1=[1.0 for j in range(0,len(words1))]
        value2=[1.0 for j in range(0,len(words2))]
        df1=pd.DataFrame(value1,index=words1,columns=['value1'])
        df2=pd.DataFrame(value2,index=words2,columns=['value2'])
        df.loc[i,'SIM2']=sim(df1,df2)

pynlpir.close()

#将dataframe中的内容写入数据库，覆盖掉原来的jg_sim表
from sqlalchemy import create_engine
conn=create_engine('mysql+pymysql://author_diff:linkerou123@localhost:3306/author_diff?charset=utf8')
sql.to_sql(df,'jg_sim',conn,schema='author_diff',if_exists='replace',index=False)
































