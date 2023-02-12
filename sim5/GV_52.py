#!/usr/bin/env python
# coding: utf-8

# In[33]:


import pandas as pd
import pandahouse as ph
import seaborn as sns
import numpy as np


# In[54]:


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230120'
}

q = """
SELECT exp_group, 
    user_id,
    sum(action = 'like') as likes,
    sum(action = 'view') as views,
    likes/views as ctr

FROM simulator_20230120.feed_actions
WHERE toDate(time) between '2023-01-01' and '2023-01-07'
    and exp_group in (1,2)
GROUP BY exp_group, user_id"""

# Вытащили пользователей
users_df = ph.read_clickhouse(q, connection=connection)


# In[55]:


# Сделаем графики в seaborn покрупнее
sns.set(rc={'figure.figsize':(11.7,8.27)})

groups = sns.histplot(data = users_df, 
              x='ctr', 
              hue='exp_group', 
              palette = ['r', 'b'],
              alpha=0.5,
              kde=False)


# ## t-test

# In[56]:


import scipy.stats as stats
stat, pvalue = stats.ttest_ind(users_df[users_df.exp_group == 1].ctr,
                users_df[users_df.exp_group == 2].ctr,
                alternative='two-sided',
                equal_var=False)
print(stat, pvalue)


# на глаз две выборки совершенно отличаются на глаз, а мы получили что нет статистически значимыx различий и это  странно
# синие распределение очень сильно скошено вправо и вторая мода может считаться очень жирным выбросом, так что сложно относиться к этому результату как вызывающему доверие, надо бы проверить и другими методами

# ## Manna - Whitneyu

# In[50]:


print(stats.mannwhitneyu(users_df[users_df.exp_group == 1].ctr,
                        users_df[users_df.exp_group == 2].ctr))


# получается, что есть статистически значимыe различия в двух выборках 

# ## Poisson_bootstraps

# In[51]:


def bootstrap(ctr1, ctr2, n_bootstrap=2000):

    poisson_bootstraps1 = stats.poisson(1).rvs(
        (n_bootstrap, len(ctr1))).astype(np.int64)

    poisson_bootstraps2 = stats.poisson(1).rvs(
            (n_bootstrap, len(ctr2))).astype(np.int64)
    
    globalCTR1 = (poisson_bootstraps1*ctr1).sum(axis=1)
    
    globalCTR2 = (poisson_bootstraps2*ctr2).sum(axis=1)

    return globalCTR1, globalCTR2


ctr1 = users_df[users_df.exp_group == 1].ctr.to_numpy()
ctr2 = users_df[users_df.exp_group == 2].ctr.to_numpy()
global_ctr1, global_ctr2 = bootstrap(ctr1,ctr2)

# sns.histplot(global_ctr1)
# sns.histplot(global_ctr2)


#Разница между глобальными CTR
sns.histplot(global_ctr1 - global_ctr2)


# In[52]:


import statistics
print("Median of data-set is : % s "
        % (statistics.median(global_ctr1 - global_ctr2)))


# разницая глобальных СТР-ов явно ствинута вправо от нуля

# ## Bucketing

# In[62]:


q = """
SELECT exp_group, bucket,
    sum(likes)/sum(views) as bucket_ctr
--    ,
--    quantileExact(0.9)(ctr) as ctr9
FROM (SELECT exp_group, 
        xxHash64(user_id)%50 as bucket,
        user_id,
        sum(action = 'like') as likes,
        sum(action = 'view') as views,
        likes/views as ctr
    FROM {db}.feed_actions 
    WHERE toDate(time) between '2023-01-01' and '2023-01-07'
        and exp_group in (1,2)
    GROUP BY exp_group, bucket, user_id)
GROUP BY exp_group, bucket
"""

df = ph.read_clickhouse(q, connection=connection)

#тест Манна-Уитни видит отличие
print(stats.mannwhitneyu(df[df.exp_group == 1].bucket_ctr, 
                   df[df.exp_group == 2].bucket_ctr, 
                   alternative = 'two-sided'))

print(stats.ttest_ind(df[df.exp_group == 1].bucket_ctr, 
                      df[df.exp_group == 2].bucket_ctr, 
                      alternative = 'two-sided',
                      equal_var=False))


# In[63]:


## Сглаженный CTR


# In[68]:


def get_smothed_ctr(user_likes, user_views, global_ctr, alpha):
    smothed_ctr = (user_likes + alpha * global_ctr) / (user_views + alpha)
    return smothed_ctr

global_ctr_1 = users_df[users_df.exp_group == 1].likes.sum()/users_df[users_df.exp_group == 1].views.sum()
global_ctr_2 = users_df[users_df.exp_group == 2].likes.sum()/users_df[users_df.exp_group == 2].views.sum()

group1 = users_df[users_df.exp_group == 1].copy()
sns.distplot(group1.ctr, kde = False)


# In[72]:


group1['smothed_ctr'] = users_df.apply(lambda x: get_smothed_ctr(x['likes'], x['views'], global_ctr_1, 5), axis=1)

sns.distplot(group1.smothed_ctr,  kde = False)


# In[73]:


group2 = users_df[users_df.exp_group == 2].copy()
sns.distplot(group2.ctr, kde = False)


# In[74]:


group2['smothed_ctr'] = users_df.apply(lambda x: get_smothed_ctr(x['likes'], x['views'], global_ctr_1, 5), axis=1)

sns.distplot(group2.smothed_ctr,  kde = False)


# In[75]:


#тест Манна-Уитни видит отличие
print(stats.mannwhitneyu(group1.ctr, 
                   group2.ctr,
                   alternative = 'two-sided'))
print(stats.mannwhitneyu(group1.smothed_ctr, 
                   group2.smothed_ctr,
                   alternative = 'two-sided'))

## t-test
print(stats.ttest_ind(group1.ctr, 
                      group2.ctr, 
                      alternative = 'two-sided',
                      equal_var=False))

print(stats.ttest_ind(group1.smothed_ctr, 
                      group2.smothed_ctr, 
                      alternative = 'two-sided',
                      equal_var=False))

get_bootstrap(
    users_df[users_df.exp_group == 1].ctr, # числовые значения первой выборки
    users_df[users_df.exp_group == 2].ctr, # числовые значения второй выборки
    boot_it = 100, # количество бутстрэп-подвыборок
    statistic = np.mean, # интересующая нас статистика
    bootstrap_conf_level = 0.95 # уровень значимости
)
# опять неоднозначный ответ, И сглаживание только слегка сработало
# хотя и в случае со сглаживанием результат похож на оригинальный с Т и U тестами
# возможно на результат второй группы повлиял еще какой-то фактор, например какие-то изменения в системе или еще один А/Б тест запущенный в тоже время и не совсем ортогональный нашему. Он мог вызвать такое смещение и жирный выброс в реzультатах CTR 

# In[ ]:




