#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pandahouse as ph
import seaborn as sns


# In[2]:


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}

q = """
SELECT exp_group, 
    user_id,
    sum(action = 'like') as likes,
    sum(action = 'view') as views,
    likes/views as ctr

FROM simulator_20230120.feed_actions
WHERE toDate(time) between '2022-12-25' and '2022-12-31'
    and exp_group in (2,3)
GROUP BY exp_group, user_id"""

# Вытащили пользователей
users_df = ph.read_clickhouse(q, connection=connection)


# In[3]:


users_df.head()


# In[4]:


# Сделаем графики в seaborn покрупнее
sns.set(rc={'figure.figsize':(11.7,8.27)})

groups = sns.histplot(data = users_df, 
              x='ctr', 
              hue='exp_group', 
              palette = ['r', 'b'],
              alpha=0.5,
              kde=False)


# In[5]:


df_pvalue = pd.DataFrame(columns =  ["stat", "pval"])
import scipy.stats as stats

number_of_tests = 10000 

for i in range(number_of_tests):
    statistic, pvalue = stats.ttest_ind(users_df[users_df.exp_group == 2].sample(500).ctr,
                    users_df[users_df.exp_group == 3].sample(500).ctr,
                    alternative='two-sided',
                    equal_var=False)
    df_pvalue.loc[i] = [statistic, pvalue]


# In[6]:


df_pvalue.head()


# In[10]:


# Сделаем графики в seaborn покрупнее
sns.set(rc={'figure.figsize':(11.7,8.27)})

sns.histplot(data = df_pvalue, 
              x='pval', bins=100,
              palette = ['b'],
              alpha=0.5,
              kde=False)


# In[8]:


# какой процент p values оказался меньше либо равен 0.05
100 *(df_pvalue[df_pvalue.pval <= 0.05].shape[0] / number_of_tests)


# Так как мы получили  меньше пяти процентов случаев p-value больше 0,05 а наши разбивки не подразумевают никаких значимых отличий в значения
# то ошибка находится в пределах случайности и наша система сплитования работает корректно

# In[ ]:




