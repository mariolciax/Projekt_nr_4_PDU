#!/usr/bin/env python
# coding: utf-8

# Praca projektowa nr 4
# ==================
# Mariola Bartosik          
# ----------------
# 
#                                                   
# 

# In[1]:


import pandas as pd
import os
import sqlite3
import tempfile


# **Otwieramy pliki csv.**

# In[2]:


Tags = pd.read_csv("data/Tags.csv")
Badges = pd.read_csv("data/Badges.csv")
Posts = pd.read_csv("data/Posts.csv")
PostLinks = pd.read_csv("data/PostLinks.csv")
Users = pd.read_csv("data/Users.csv")
Votes = pd.read_csv("data/Votes.csv")
Comments = pd.read_csv("data/Comments.csv")


# In[3]:


# sciezka dostępu do bazy danych:
base = os.path.join(tempfile.mkdtemp(), 'przyklad.db')
if os.path.isfile(base): # jesli baza już istneje...
    os.remove(base) # ...usuniemy ja


# In[4]:


conn = sqlite3.connect(base) # połączenie do bazy danych


# In[5]:


Badges.to_sql("Badges", conn) # importujemy ramkę danych do bazy danych
Comments.to_sql("Comments", conn)
PostLinks.to_sql("PostLinks", conn)
Posts.to_sql("Posts", conn)
Tags.to_sql("Tags", conn)
Users.to_sql("Users", conn)
Votes.to_sql("Votes", conn)


# Zadanie 1
# ------------
# 

# ### 1. Rozwiązanie za pomocą wywołania *pandas.read_sql_query("""zapytanie SQL""")*

# In[6]:


df_sql_1 = pd.read_sql_query("""
                    SELECT Title, Score, ViewCount, FavoriteCount
                    FROM Posts
                    WHERE PostTypeId=1 AND FavoriteCount >= 25 AND ViewCount>=10000
                    """, conn)
df_sql_1 


# ### 2. Rozwiązanie za pomocą wywołania ciągu „zwykłych” metod i funkcji z pakietu *pandas*.
# 

# In[7]:


df_pd_1 = (Posts[(Posts.PostTypeId == 1) & (Posts.FavoriteCount >= 25) & (Posts.ViewCount >= 10000)]
[['Title', 'Score', 'ViewCount','FavoriteCount']])
df_pd_1.reset_index(drop=True, inplace=True)
df_pd_1


# ### 3. Sprawdzenie tożsamości zwracanych wyników

# In[8]:


df_pd_1.equals(df_sql_1)


# Zadanie 2
# ---------

# ### 1. Rozwiązanie za pomocą wywołania *pandas.read_sql_query("""zapytanie SQL""")*

# In[9]:


df_sql_2 = pd.read_sql_query("""
                    SELECT Posts.Title, RelatedTab.NumLinks FROM
                        (SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks
                        FROM PostLinks GROUP BY RelatedPostId) AS RelatedTab
                    JOIN Posts ON RelatedTab.PostId=Posts.Id
                    WHERE Posts.PostTypeId=1
                    ORDER BY NumLinks DESC
                    LIMIT 10
                    """, conn)
df_sql_2


# ### 2. Rozwiązanie za pomocą wywołania ciągu „zwykłych” metod i funkcji z pakietu *pandas*.
# 

# In[10]:


RelatedTab = PostLinks.RelatedPostId.to_frame('PostId')
RelatedTab = RelatedTab.groupby(['PostId']).size().to_frame('NumLinks')
Post = Posts[Posts.PostTypeId == 1]
df_pd_2 = (pd.merge(Post, RelatedTab, left_on='Id', right_on='PostId')
           [[ 'Title', 'NumLinks']])
df_pd_2 =df_pd_2.nlargest(10, columns='NumLinks').reset_index(drop = True)
df_pd_2


# ### 3. Sprawdzenie tożsamości zwracanych wyników

# In[11]:


df_pd_2.equals(df_sql_2)


# Zadanie 3
# ---------

# ### 1. Rozwiązanie za pomocą wywołania *pandas.read_sql_query("""zapytanie SQL""")*

# In[12]:


df_sql_3 = pd.read_sql_query("""
                    SELECT DISTINCT
                        Users.Id,
                        Users.DisplayName,
                        Users.Reputation,
                        Users.Age,
                        Users.Location
                    FROM (
                            SELECT
                                Name, UserID
                            FROM Badges
                            WHERE Name IN (
                                SELECT
                                    Name
                                FROM Badges
                                WHERE Class=1
                                GROUP BY Name
                                HAVING COUNT(*) BETWEEN 2 AND 10
                            )
                            AND Class=1
                        ) AS ValuableBadges
                    JOIN Users ON ValuableBadges.UserId=Users.Id
                    """, conn)
df_sql_3


# ### 2. Rozwiązanie za pomocą wywołania ciągu „zwykłych” metod i funkcji z pakietu *pandas*.
# 

# In[13]:


Name = (Badges[Badges.Class == 1].groupby('Name').filter(lambda g: len(g) >= 2)
           .groupby('Name').filter(lambda g: len(g) <= 10).groupby('Name').size().reset_index())
ValuableBadges = Badges[Badges.Name.isin(list(Name.Name))]
ValuableBadges = ValuableBadges[ValuableBadges.Class == 1][[ 'Name', 'UserId' ]]
df_pd_3 = ((pd.merge(ValuableBadges, Users, left_on='UserId', right_on='Id'))
            [['Id', 'DisplayName','Reputation','Age','Location']])
df_pd_3 = df_pd_3.drop_duplicates(subset = ['Id', 'DisplayName','Reputation','Age','Location'])
df_pd_3.reset_index(drop = True, inplace=True)
df_pd_3


# ### 3. Sprawdzenie tożsamości zwracanych wyników

# In[14]:


df_pd_3.equals(df_sql_3)

