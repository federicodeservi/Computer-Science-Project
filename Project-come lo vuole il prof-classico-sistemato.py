#!/usr/bin/env python
# coding: utf-8

# # Problemi:
# 
# ### Rileggere il tutto e sistemare commenti (in particolare punto 9)
# ### Confronto

# ### 1. Normalize the loan_lenders table. In the normalized table, each row must have one loan_id and one lender.

# In[1]:


#Versione Python: 3
#Versione pandas: 0.25.2
#Versione numpy: 1.17.3

import pandas as pd
import numpy as np
import datetime


# In[2]:


loans_lenders = pd.read_csv(r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\loans_lenders.csv")


# In[3]:


#loans_lenders.head()


# In[4]:


#La funzione explode è presente soltanto da pandas 0.25. Assegna la nuova colonna a cui ho effettuato una split alla vecchia colonna lenders. Poi applico la funzione explode.

loans_lenders_norm = loans_lenders.assign(lenders=loans_lenders["lenders"].str.split(',')).explode("lenders")


# In[5]:


#loans_lenders_norm.head()


# ### 2. For each loan, add a column duration corresponding to the number of days between the disburse time and the planned expiration time.

# In[6]:


#Leggo il file
#Trasformo le seguenti colonne in datetime format per fare la differenza tra date successivamente 

loans = pd.read_csv(r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\loans.csv", parse_dates=["disburse_time", "planned_expiration_time"])
pd.set_option('display.max_columns', None)


# In[7]:


#Visualizzo il tipo di dati contenuti nel dataframe

#loans.dtypes


# In[8]:


#Calcolo la differenza tra expiration e disburse nel dataset loans_not_nan e la inserisco nella colonna apposita

loans["duration"] = loans["planned_expiration_time"] - loans["disburse_time"]


# In[9]:


#loans.head()


# ### 3. Find the lenders that have funded at least twice.

# In[10]:


#Per prima cosa unisco i df loans_lenders_norm e loans per sapere se tutti lenders in loans_lenders_norm corrispondano a uno status "funded"

loans_lenders_merged = pd.merge(loans_lenders_norm, loans[["loan_id", "status"]], on="loan_id", how="left")


# In[11]:


#Poi seleziono solo le righe che hanno come status "funded"

loans_lenders_merged_funded = loans_lenders_merged[loans_lenders_merged["status"] == "funded"]


# In[12]:


#E ora seleziono i "lenders who have funded at least twice"

lenders_twice =  loans_lenders_merged_funded["lenders"].value_counts().reset_index(name="count").query("count >= 2")


# In[13]:


#lender_twice.head()


# ### 4. For each country, compute how many loans have involved that country as borrowers.
# 

# In[14]:


#Innanzitutto creo un dataframe che contenga solo le righe di loans che:
#1) non hanno valori null in "duration"
#2) non hanno valori in "planned_expiration_time" che siano minori di "disburse_time" 
#(questo non avrebbe alcun senso: potrei scambiarli o fare altre manipolazioni ma voglio essere conservativo, quindi ignoro quelle poche righe che hanno questa anomalia)
#in modo da evitare errori

loans_not_null = loans[loans["duration"].isnull() == False]
loans_not_null = loans_not_null[loans_not_null["planned_expiration_time"] > loans_not_null["disburse_time"]]


# In[15]:


#Uso una groupby, a cui applico count. Rinonimo infine la colonna otennuta.

num_loans_country = loans_not_null.groupby("country_name").count().reset_index()[["country_name", "loan_id"]]
num_loans_country = num_loans_country.rename(columns={"loan_id" : "count"})


# In[16]:


#num_loans_country.head()


# ### 5. For each country, compute the overall amount of money borrowed.

# In[17]:


#Uso una groupby, a cui applico una sum sulla colonna "loan_amount". Rinomino la colonna.

tot_borr_country = loans_not_null.groupby("country_name")["loan_amount"].sum().reset_index().rename(columns={"loan_amount" : "borrowed_amount"})


# In[18]:


#tot_borr_country.head()


# ### 6. Like the previous point, but expressed as a percentage of the overall amount lent.
# 

# In[19]:


#Calcolo l'ammontare totale lent_tot, sommano gli elementi della colonna "loan_amount". Eseguo i passaggi del punto precedente, didendo per tale totale e moltiplicando per 100. Rinonimo poi la colonna.

lent_tot = loans_not_null["loan_amount"].sum()

tot_borr_country_perc = loans_not_null.groupby("country_name")["loan_amount"].sum().reset_index()
tot_borr_country_perc["loan_amount"] = tot_borr_country_perc["loan_amount"]/lent_tot*100
tot_borr_country_perc = tot_borr_country_perc.rename(columns={"loan_amount" : "loan_amount_perc"})


# In[20]:


#tot_borr_country_perc.head()


# ***

# In[21]:


#Unisco i tre precedenti dataframe. Selezione solo le colonne che non sono duplicate.

country_statistics = pd.concat([num_loans_country, tot_borr_country, tot_borr_country_perc], axis=1)

print (~country_statistics.columns.duplicated())

country_statistics = country_statistics.loc[:, ~country_statistics.columns.duplicated()]

country_statistics.head()


# ***

# ### 7. Like the three previous points, but split for each year.
# 

# In[22]:


#Converto disburse_time in un formato datetime. Imposto quella colonna come indice.

loans_not_null["disburse_time"] =  pd.to_datetime(loans_not_null["disburse_time"])
loans_not_null = loans_not_null.set_index("disburse_time")


# In[23]:


#Eseguo una groupby usando il grouper con freq. pari ad un anno. Applico una sum.

loans_by_year_sum = loans_not_null.groupby(["country_name", pd.Grouper(freq="Y")])["loan_amount"].sum().to_frame()


# In[24]:


#loans_by_year_sum.head()


# In[25]:


#Come sopra ma calcolo la percentuale.

loans_by_year_perc = loans_by_year_sum
loans_by_year_perc["loan_amount"] = loans_by_year_perc["loan_amount"]/lent_tot*100
loans_by_year_perc = loans_by_year_perc.rename(columns={"loan_amount" : "loan_amount_perc"})


# In[26]:


#loans_by_year_perc.head()


# In[27]:


#Come sopra ma applico un count e rinomino la colonna relativa.

loans_by_year_count = loans_not_null.groupby(["country_name", pd.Grouper(freq="Y")])["loan_id"].count().to_frame()
loans_by_year_count = loans_by_year_count.rename(columns={"loan_id" : "count"})


# In[28]:


#loans_by_year_count.head()


# ***

# In[29]:


#Unisco i tre precedenti dataframe. Selezione solo le colonne che non sono duplicate.

country_statistics_by_year = pd.concat([loans_by_year_count, loans_by_year_sum, loans_by_year_perc], axis=1)

print (~country_statistics_by_year.columns.duplicated())

country_statistics_by_year = country_statistics_by_year.loc[:, ~country_statistics_by_year.columns.duplicated()]

country_statistics_by_year.head()


# ***

# ### 8. For each lender, compute the overall amount of money lent.

# In[30]:


#Creo il df "lenders_num" che contiene, per ogni loan_id, il numero di lenders coinvolti (lenders_count)

lenders_num = loans_lenders_norm.groupby("loan_id").count().reset_index().rename(columns={"lenders" : "lenders_count"})


# In[31]:


#Unisco i due dataframe in modo da avere le informazioni: loan_id, numero di lenders coinvolti e loan_amount

lenders_num_details = pd.merge(lenders_num, loans_not_null, on="loan_id")[["loan_id", "lenders_count", "loan_amount"]]


# In[32]:


#Aggiungo una colonna in cui calcolo l'ammontare per lender, assumendo che tutti abbiano contribuito in egual misura

lenders_num_details["amount_per_person"] = lenders_num_details["loan_amount"] / lenders_num_details["lenders_count"]


# In[33]:


#lenders_num_details.head()


# In[34]:


#loans_lenders_norm.head()


# In[35]:


#Ora unisco loans_lenders_norm e lenders_num_details in modo da avere un df come il loans_lenders_norm originale, ma che abbia una colonna 
#che indichi il "amount_per_person"

loans_lenders_merged = pd.merge(loans_lenders_norm, lenders_num_details, on="loan_id", how="left")


# In[36]:


#loans_lenders_merged.head()


# In[37]:


#Ora raggruppo per lender e sommo, ottenendo il totale prestato da ogni lender.

lenders_overall_lent = loans_lenders_merged.groupby("lenders")["amount_per_person"].sum().to_frame().reset_index()


# In[38]:


#lenders_overall_lent.head()


# ### 9. For each country, compute the difference between the overall amount of money lent and the overall amount of money borrowed. Since the country of the lender is often unknown, you can assume that the true distribution among the countries is the same as the one computed from the rows where the country is known.
# 

# In[39]:


#Per prima cosa devo sistemare il dataset, in modo tale da averlo completo, senza valore Null. 
#Divido quindi il dataset in due parti, una con le modalità delle colonne di country conosciuta e una no.
#Metodo: Calcolo poi la percentuale delle varie nazionalità nel primo dataset e le applico in modo randomico al secondo dataset.
#Riunifico poi i due dataset

lenders = pd.read_csv(r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\lenders.csv")


# In[40]:


#lenders.head()


# In[41]:


lenders_notnull = lenders.loc[lenders["country_code"].notnull()].reset_index()                         


# In[42]:


#lenders_notnull.head()


# In[43]:


lenders_null = lenders.loc[lenders["country_code"].isnull()].reset_index()


# In[44]:


#lenders_null.head()


# In[45]:


#Calcolo la distribuzione di nazioni nel dataset lenders_notnull

tot_notnull_users = len(lenders_notnull.index)
print(tot_notnull_users)


# In[46]:


country_ripartition = lenders_notnull[["index", "country_code"]].groupby("country_code").count().reset_index().rename(columns = {"index":"n_users"})
country_ripartition["percentage"] = country_ripartition["n_users"]/tot_notnull_users*100


# In[47]:


#country_ripartition.head()


# In[48]:


#Ora riempio il dataset lenders_null con la funzione np.random.choice (inserendo come seed '1234')
#ma per fare questo devo normalizzare le percentuali dividendole per la loro somma, altrimenti otterei l'errore (probabilities do not sum to 1

country_ripartition["percentage"] /= country_ripartition["percentage"].sum()


# In[49]:


np.random.seed(1234)

lenders_null["country_code"] = np.random.choice(country_ripartition["country_code"], size=len(lenders_null.index), p = country_ripartition["percentage"])


# In[50]:


#lenders_null.head()


# In[51]:


#Riunisco i due dataset nel dataset originario

lenders = pd.concat([lenders_notnull, lenders_null]).drop(columns="index")


# In[52]:


#lenders.head()


# ### Ora che ho il dataset sistemato, proseguo con il punto 9

# In[53]:


#Unisco al dataframe lenders_overall_lent la colonna country_code, usando una merge.

tot_lent_country = pd.merge(lenders_overall_lent, lenders[["permanent_name", "country_code"]], left_on="lenders", right_on="permanent_name").drop(columns="permanent_name").groupby("country_code")["amount_per_person"].sum().to_frame().rename(columns={"amount_per_person" : "lent_amount"}).reset_index()


# In[54]:


#tot_lent_country.head()


# In[55]:


#Carico il file country_stats.csv.

country_stats = pd.read_csv(r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\country_stats.csv")


# In[56]:


#XXXX

tot_borr_country = pd.merge(tot_borr_country, country_stats[["country_name", "country_code"]], on="country_name")


# In[57]:


#tot_borr_country.head()


# In[58]:


country_lent_borr = pd.merge(tot_lent_country, tot_borr_country, on="country_code")
country_lent_borr = country_lent_borr[["country_name", "country_code", "lent_amount", "borrowed_amount"]]


# In[59]:


#country_lent_borr.head()


# In[60]:


#Calcolo la differenza tra il totale presentato e il totale ricevuto in valore assoluto

country_lent_borr["difference (abs)"] = abs(country_lent_borr["lent_amount"] - country_lent_borr["borrowed_amount"])


# In[61]:


#country_lent_borr.head()


# ### 10. Which country has the highest ratio between the difference computed at the previous point and the population?
# 

# In[62]:


country_lent_borr  = pd.merge(country_lent_borr, country_stats[["country_code", "population"]], on="country_code")


# In[63]:


#Calcolo il ratio, inserendolo in una colonna apposita

country_lent_borr["ratio"] = country_lent_borr["difference (abs)"]/country_lent_borr["population"]


# In[64]:


#country_lent_borr.loc[country_lent_borr["ratio"].idxmax()].to_frame()


# In[65]:


#Trovo la riga che corrisponde al massimo valore di ratio

country_lent_borr[country_lent_borr['ratio']==country_lent_borr['ratio'].max()]


# ### 11. Which country has the highest ratio between the difference computed at point 9 and the population that is not below the poverty line?
# 

# In[66]:


country_lent_borr  = pd.merge(country_lent_borr, country_stats[["country_code", "population"]], on="country_code")


# In[67]:


#Potrei riempire i valori mancanti di "population_below_poverty_line" con la media dei valori della colonna
#country_stats["population_below_poverty_line"] = country_stats["population_below_poverty_line"].fillna(country_stats["population_below_poverty_line"].mean())

#Mantengo un approccio conservativo che ho seguito fino ad ora e seleziono solo le countries che non hanno nan in quella colonna

country_stats_not_null = country_stats[country_stats["population_below_poverty_line"].isnull() == False]


# In[68]:


#Calcolo le persone al di sopra della linea di povertà, aggiungendole ad una colonna

country_stats_not_null["population_above_poverty_line"] = (country_stats_not_null["population"]-(country_stats_not_null["population"]*country_stats_not_null["population_below_poverty_line"]/100))


# In[69]:


#country_stats_not_null.head()


# In[70]:


country_lent_borr  = pd.merge(country_lent_borr, country_stats_not_null[["country_code", "population_above_poverty_line"]], on="country_code")


# In[71]:


#Calcolo il ratio (sopra linea di povertà), inserendolo in una colonna apposita

country_lent_borr["ratio_above_poverty"] = country_lent_borr["difference (abs)"]/country_lent_borr["population_above_poverty_line"]


# In[72]:


#Trovo la riga che corrisponde al massimo valore di ratio_above_poverty

country_lent_borr[country_lent_borr['ratio_above_poverty']==country_lent_borr['ratio_above_poverty'].max()]


# ### 12. For each year, compute the total amount of loans. Each loan that has planned expiration time and disburse time in different years must have its amount distributed proportionally to the number of days in each year. 
# 

# In[73]:


#Resetto l'indice

loans_not_null = loans_not_null.reset_index()


# In[75]:


#Rimuovo il fuso orario e le ore per facilitare i calcoli successivi tra date ed evitare errori

loans_not_null["disburse_time"] = loans_not_null["disburse_time"].dt.tz_localize(None)
loans_not_null["planned_expiration_time"] = loans_not_null["planned_expiration_time"].dt.tz_localize(None)


# In[79]:


loans_not_null["disburse_time"] = loans_not_null["disburse_time"].dt.normalize()
loans_not_null["planned_expiration_time"] = loans_not_null["planned_expiration_time"].dt.normalize()


# In[80]:


#Divido loans in loans_same_year (disburse e expiration hanno lo stesso anno) e loans_diff_year

loans_same_year= loans_not_null[loans_not_null["disburse_time"].dt.year == loans_not_null["planned_expiration_time"].dt.year][["loan_id" , "disburse_time", "planned_expiration_time","loan_amount"]]
loans_diff_year = loans_not_null[loans_not_null["disburse_time"].dt.year != loans_not_null["planned_expiration_time"].dt.year][["loan_id" ,"disburse_time", "planned_expiration_time","loan_amount"]]


# In[ ]:


#Creo un dataframe vuoto che andrò a riempire successivamente, le cui colonne corrispondono agli anni. 
#Gli anni vanno da __ a ___ :

years_min_max = [loans_not_null["planned_expiration_time"].max().year, loans_not_null["planned_expiration_time"].min().year,
                 loans_not_null["disburse_time"].max().year, loans_not_null["disburse_time"].min().year]

print(max(years_min_max), min(years_min_max))


# In[ ]:


year_amount = pd.DataFrame(0, index=[0], columns=["2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"])


# In[ ]:


def loan_func_same_years(df):  
    year_amount[df["disburse_time"].strftime("%Y")] += df["loan_amount"]


# In[ ]:


#NB. uso .disburse invece che ["disburse..."] perchè più veloce.

def loan_func_diff_years(df):  
    completed_loop=0
    start_date = df.disburse_time
    exp_date = df.planned_expiration_time
    year = [df["disburse_time"].strftime("%Y")]
    tot_days = (exp_date - start_date).days+1
    year_amount[year] += (df.loan_amount*((datetime.datetime(start_date.year, 12, 31) - start_date).days+1) / (tot_days))
    start_date = datetime.datetime(start_date.year+1, 1, 1)
    year = [start_date.strftime("%Y")]
    while completed_loop != 1:
        if start_date.year == exp_date.year:
                year_amount[year] += (df.loan_amount*(exp_date - start_date).days+1) / (tot_days)
                completed_loop=1
        else:
                year_amount[year] += (df.loan_amount*(datetime.datetime(start_date.year, 12, 31) - start_date).days+1) / (tot_days)
                start_date = datetime.datetime(start_date.year+1, 1, 1)
                year = [start_date.strftime("%Y")]


# In[ ]:


loans_same_year.apply(loan_func_same_years, axis=1)


# In[ ]:


year_amount


# In[ ]:


loans_diff_year.apply(loan_func_diff_years, axis=1)


# In[ ]:


year_amount


# ***

# Tempo totale sul pc fisso = 2 min precisi

# ***
