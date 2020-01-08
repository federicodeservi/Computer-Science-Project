#!/usr/bin/env python
# coding: utf-8

# # Progetto di Computer Science

# #### Nome: Federico
# #### Cognome: De Servi
# #### Matricola: 812166

# In[1]:


#Versione Python: 3
#Versione pandas: 0.25.2

import pandas as pd
import numpy as np
import datetime
from IPython.display import display


# ### 0. Change paths for the following datasets

# In[2]:


#Cambiate i path dei seguenti dataset con i path che avete sulla vostra macchina

#Dataset: "loans.csv"
loans_path = r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\loans.csv"
#Dataset: "lenders.csv"
lenders_path = r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\lenders.csv"
#Dataset: "loans_lenders.csv"
loans_lenders_path = r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\loans_lenders.csv"
#Dataset: "country_stats.csv"
country_stats_path = r"C:\Users\Federico De Servi\Google Drive\Universita\Materie\Computer Science\Materiali - archivio\project\country_stats.csv" 


# ### 1. Normalize the loan_lenders table. In the normalized table, each row must have one loan_id and one lender.

# In[3]:


#Leggo il file loans_lenders

loans_lenders = pd.read_csv(loans_lenders_path)


# In[4]:


#display(loans_lenders.head())


# In[5]:


#La funzione explode è presente soltanto da pandas 0.25. Essa fa "esplodere" liste in righe separate. (vedi docs) 
#Siccome ogni riga della colonna "lenders" è composta da stringhe separate da virgola, devo effettuare una split(",") e poi applicare la funzione explode sulla colonna lenders.

loans_lenders_norm = loans_lenders.assign(lenders=loans_lenders["lenders"].str.split(",")).explode("lenders")


# In[6]:


#Rimuovo i whitespace che si trovano all'inizio e alla fine di ogni stringa della colonna "lenders, usando str.strip()
#Questo perchè andrebbero a creare problemi successivamente:
#se volessi cercare un lender che inizia con uno spazio bianco avrei difficoltà. Meglio evitare.

loans_lenders_norm["lenders"] = loans_lenders_norm["lenders"].str.strip()


# In[7]:


#display(loans_lenders_norm.head())


# ### 2. For each loan, add a column duration corresponding to the number of days between the disburse time and the planned expiration time.

# In[8]:


#Leggo il file
#Trasformo le seguenti colonne in datetime format per fare la differenza tra date successivamente 

loans = pd.read_csv(loans_path, parse_dates=["disburse_time", "planned_expiration_time"])
pd.set_option('display.max_columns', None)


# In[9]:


#Visualizzo il tipo di dati contenuti nel dataframe
#loans.dtypes


# In[10]:


#Calcolo la differenza tra expiration e disburse nel dataframe loans_not_nan e la inserisco nella colonna apposita

loans["duration"] = loans["planned_expiration_time"] - loans["disburse_time"]


# In[11]:


#display(loans.head())


# ### 3. Find the lenders that have funded at least twice.

# In[12]:


#Per prima cosa unisco i df loans_lenders_norm e loans per sapere se tutti lenders in loans_lenders_norm corrispondano a uno status "funded"

loans_lenders_merged = pd.merge(loans_lenders_norm, loans[["loan_id", "status"]], on="loan_id", how="left")


# In[13]:


#Poi seleziono solo le righe che hanno come status "funded"

loans_lenders_merged_funded = loans_lenders_merged[loans_lenders_merged["status"] == "funded"]


# In[14]:


#E ora seleziono i "lenders who have funded at least twice"

lenders_twice =  loans_lenders_merged_funded["lenders"].value_counts().reset_index(name="count").query("count >= 2")


# In[15]:


#display(lenders_twice.head())


# ### 4. For each country, compute how many loans have involved that country as borrowers.
# 

# In[16]:


#Innanzitutto creo un dataframe che contenga solo le righe di loans che:
#1) non hanno valori null in "duration"
#2) non hanno valori in "planned_expiration_time" che siano minori di "disburse_time" 
#(questo non avrebbe alcun senso: potrei scambiarli o fare altre manipolazioni ma voglio essere conservativo, quindi ignoro quelle poche righe che hanno questa anomalia)
#in modo da evitare errori) 

loans_not_null = loans[loans["duration"].isnull() == False]
loans_not_null = loans_not_null[loans_not_null["planned_expiration_time"] > loans_not_null["disburse_time"]]


# In[17]:


#Uso una groupby, a cui applico count. Rinonimo infine la colonna otennuta.

num_loans_country = loans_not_null.groupby("country_name").count().reset_index()[["country_name", "loan_id"]]
num_loans_country = num_loans_country.rename(columns={"loan_id" : "count"})


# In[18]:


#display(num_loans_country.head())


# ### 5. For each country, compute the overall amount of money borrowed.

# In[19]:


#Uso una groupby, a cui applico una sum sulla colonna "loan_amount". Rinomino la colonna.

tot_borr_country = loans_not_null.groupby("country_name")["loan_amount"].sum().reset_index().rename(columns={"loan_amount" : "borrowed_amount"})


# In[20]:


#display(tot_borr_country.head())


# ### 6. Like the previous point, but expressed as a percentage of the overall amount lent.
# 

# In[21]:


#Calcolo l'ammontare totale lent_tot, sommano gli elementi della colonna "loan_amount". Eseguo i passaggi del punto precedente, didendo per tale totale e moltiplicando per 100. Rinonimo poi la colonna.

lent_tot = loans_not_null["loan_amount"].sum()

tot_borr_country_perc = loans_not_null.groupby("country_name")["loan_amount"].sum().reset_index()
tot_borr_country_perc["loan_amount"] = tot_borr_country_perc["loan_amount"]/lent_tot*100
tot_borr_country_perc = tot_borr_country_perc.rename(columns={"loan_amount" : "loan_amount_perc"})


# In[22]:


#display(tot_borr_country_perc.head())


# ***

# In[23]:


#Unisco i tre precedenti dataframe. Selezione solo le colonne che non sono duplicate.

country_statistics = pd.concat([num_loans_country, tot_borr_country, tot_borr_country_perc], axis=1)

print (~country_statistics.columns.duplicated())

country_statistics = country_statistics.loc[:, ~country_statistics.columns.duplicated()]

display(country_statistics.head())


# ***

# ### 7. Like the three previous points, but split for each year.
# 

# In[24]:


#Converto disburse_time in un formato datetime. Imposto quella colonna come indice.

loans_not_null["disburse_time"] =  pd.to_datetime(loans_not_null["disburse_time"])
loans_not_null = loans_not_null.set_index("disburse_time")


# In[25]:


#Eseguo una groupby usando il grouper con freq. pari ad un anno. Applico una sum.

loans_by_year_sum = loans_not_null.groupby(["country_name", pd.Grouper(freq="Y")])["loan_amount"].sum().to_frame()


# In[26]:


#display(loans_by_year_sum.head())


# In[27]:


#Come sopra ma calcolo la percentuale.

loans_by_year_perc = loans_by_year_sum
loans_by_year_perc["loan_amount"] = loans_by_year_perc["loan_amount"]/lent_tot*100
loans_by_year_perc = loans_by_year_perc.rename(columns={"loan_amount" : "loan_amount_perc"})


# In[28]:


#display(loans_by_year_perc.head())


# In[29]:


#Come sopra ma applico un count e rinomino la colonna relativa.

loans_by_year_count = loans_not_null.groupby(["country_name", pd.Grouper(freq="Y")])["loan_id"].count().to_frame()
loans_by_year_count = loans_by_year_count.rename(columns={"loan_id" : "count"})


# In[30]:


#display(loans_by_year_count.head())


# ***

# In[31]:


#Unisco i tre precedenti dataframe. Selezione solo le colonne che non sono duplicate.

country_statistics_by_year = pd.concat([loans_by_year_count, loans_by_year_sum, loans_by_year_perc], axis=1)

print (~country_statistics_by_year.columns.duplicated())

country_statistics_by_year = country_statistics_by_year.loc[:, ~country_statistics_by_year.columns.duplicated()]

display(country_statistics_by_year.head())


# ***

# ### 8. For each lender, compute the overall amount of money lent.

# In[32]:


#Creo il df "lenders_num" che contiene, per ogni loan_id, il numero di lenders coinvolti (lenders_count)

lenders_num = loans_lenders_norm.groupby("loan_id").count().reset_index().rename(columns={"lenders" : "lenders_count"})


# In[33]:


#Unisco i due dataframe in modo da avere le informazioni: loan_id, numero di lenders coinvolti e loan_amount

lenders_num_details = pd.merge(lenders_num, loans_not_null, on="loan_id")[["loan_id", "lenders_count", "loan_amount"]]


# In[34]:


#Aggiungo una colonna in cui calcolo l'ammontare per lender, assumendo che tutti abbiano contribuito in egual misura

lenders_num_details["amount_per_person"] = lenders_num_details["loan_amount"] / lenders_num_details["lenders_count"]


# In[35]:


#display(lenders_num_details.head())


# In[36]:


#display(loans_lenders_norm.head())


# In[37]:


#Ora unisco loans_lenders_norm e lenders_num_details in modo da avere un df come il loans_lenders_norm originale, ma che abbia una colonna 
#che indichi il "amount_per_person"

loans_lenders_merged = pd.merge(loans_lenders_norm, lenders_num_details, on="loan_id", how="left")


# In[38]:


#display(loans_lenders_merged.head())


# In[39]:


#Ora raggruppo per lender e sommo, ottenendo il totale prestato da ogni lender.

lenders_overall_lent = loans_lenders_merged.groupby("lenders")["amount_per_person"].sum().to_frame().reset_index()


# In[40]:


#display(lenders_overall_lent.head())


# ### 9. For each country, compute the difference between the overall amount of money lent and the overall amount of money borrowed. Since the country of the lender is often unknown, you can assume that the true distribution among the countries is the same as the one computed from the rows where the country is known.
# 

# In[41]:


#Per prima cosa devo sistemare il dataframe, in modo tale da averlo completo, senza valore Null. 
#Divido quindi il dataframe in due parti, una con le modalità delle colonne di country conosciuta e una no.
#Metodo: Calcolo poi la percentuale delle varie nazionalità nel primo dataframe e le applico in modo randomico al secondo dataframe.
#Riunifico poi i due dataframe

lenders = pd.read_csv(lenders_path)


# In[42]:


#display(lenders.head())


# In[43]:


lenders_notnull = lenders.loc[lenders["country_code"].notnull()].reset_index()                         


# In[44]:


#display(lenders_notnull.head())


# In[45]:


lenders_null = lenders.loc[lenders["country_code"].isnull()].reset_index()


# In[46]:


#display(lenders_null.head())


# In[47]:


#Calcolo la distribuzione di nazioni nel dataframe lenders_notnull

tot_notnull_users = len(lenders_notnull.index)
print(tot_notnull_users)


# In[48]:


#Calcolo la distribuzione delle nazioni nel dataframe lenders_notnull

country_ripartition = lenders_notnull[["index", "country_code"]].groupby("country_code").count().reset_index().rename(columns = {"index":"n_users"})
country_ripartition["percentage"] = country_ripartition["n_users"]/tot_notnull_users*100


# In[49]:


#display(country_ripartition.head())


# In[50]:


#Ora riempio il dataframe lenders_null in modo che abbia la stessa distribuzione di lenders_notnull.
#Per fare questo uso la funzione np.random.choice (inserendo come seed '1234')
#In questo modo assegno la nazionalità in modo randomico e non sistematico (x. es. partendo dall'alto), ottenendo la distribuzione finale che voglio ottenere
#ma per fare questo devo normalizzare le percentuali dividendole per la loro somma, altrimenti otterei l'errore (probabilities do not sum to 1

country_ripartition["percentage"] /= country_ripartition["percentage"].sum()


# In[51]:


#Preseguo

np.random.seed(1234)
lenders_null["country_code"] = np.random.choice(country_ripartition["country_code"], size=len(lenders_null.index), p = country_ripartition["percentage"])


# In[52]:


#display(lenders_null.head())


# In[53]:


#Riunisco i due dataframe nel dataframe originario

lenders = pd.concat([lenders_notnull, lenders_null]).drop(columns="index")


# In[54]:


#display(lenders.head())


# ### Ora che ho il dataframe sistemato, proseguo con il punto 9

# In[55]:


#Integro i dataframe "lenders", il quale ha i nomi dei lenders e la loro nazionalità, e il dataframe "lenders_overall_lent" che indica per ogni lender l'ammontare "lent"
#A quel punto raggruppo per nazionalità e effettuo una somma, ottenendo l'ammontare "lent" per ogni nazione
#Inserisco queste info nel dataframe "tot_lent_country"

tot_lent_country = pd.merge(lenders_overall_lent, lenders[["permanent_name", "country_code"]], left_on="lenders", right_on="permanent_name").drop(columns="permanent_name").groupby("country_code")["amount_per_person"].sum().to_frame().rename(columns={"amount_per_person" : "lent_amount"}).reset_index()


# In[56]:


#display(tot_lent_country.head())


# In[57]:


#Nel dataframe "tot_lent_country" ho indicato il country_code, mentre nel dataframe "tot_borr_country" ho il nome della nazione
#Devo "normalizzare" queste differenze, quindi integro tot_borr_country con "country_stats", in modo da avere anche lì il country code

country_stats = pd.read_csv(country_stats_path)
tot_borr_country = pd.merge(tot_borr_country, country_stats[["country_name", "country_code"]], on="country_name")


# In[58]:


#display(tot_borr_country.head())


# In[59]:


#Ora creo un df che contenga l'ammontare di denaro "lent" e "borrowed" per ogni nazione
#Devo quindi fare una merge tra tot_lent_country e tot_borr_country

country_lent_borr = pd.merge(tot_lent_country, tot_borr_country, on="country_code")
country_lent_borr = country_lent_borr[["country_name", "country_code", "lent_amount", "borrowed_amount"]]


# In[60]:


#Calcolo la differenza tra il totale presentato e il totale ricevuto

country_lent_borr["difference"] = (country_lent_borr["lent_amount"] - country_lent_borr["borrowed_amount"])


# In[61]:


#display(country_lent_borr.head())


# ### 10. Which country has the highest ratio between the difference computed at the previous point and the population?
# 

# In[62]:


#Per risolvere questo punto devo integrare il df country_lent_borr con info circa la popolazione, che ho nel df country_stats
#Quindi integro facendo una merge.

country_lent_borr  = pd.merge(country_lent_borr, country_stats[["country_code", "population"]], on="country_code")


# In[63]:


#Calcolo il ratio, inserendolo in una colonna apposita

country_lent_borr["ratio"] = country_lent_borr["difference"]/country_lent_borr["population"]


# In[64]:


#Trovo la riga che corrisponde al massimo valore di ratio

country_lent_borr[country_lent_borr['ratio']==country_lent_borr['ratio'].max()]


# ### 11. Which country has the highest ratio between the difference computed at point 9 and the population that is not below the poverty line?
# 

# In[65]:


#Il problema principale è che "population_below_poverty_line" presenta valori nulli, rendendo impossibile la soluzione del punto
#Potrei riempire i valori mancanti di "population_below_poverty_line" con la media dei valori della colonna, facendo così:
#country_stats["population_below_poverty_line"] = country_stats["population_below_poverty_line"].fillna(country_stats["population_below_poverty_line"].mean())

#Mantengo l'approccio conservativo che ho seguito fino ad ora e seleziono solo le countries che non hanno nan in quella colonna

country_stats_not_null = country_stats[country_stats["population_below_poverty_line"].isnull() == False]


# In[66]:


#Calcolo le persone al di sopra della linea di povertà, aggiungendole ad una colonna

country_stats_not_null["population_above_poverty_line"] = (country_stats_not_null["population"] -(country_stats_not_null["population"]*country_stats_not_null["population_below_poverty_line"]/100))


# In[67]:


#display(country_stats_not_null.head())


# In[68]:


#A questo punto aggiungo il dato "population_above_poverty_line" al df country_lent_borr.
#Nota bene: sto eseguendo una merge inner, per cui nel df saranno presenti solo le righe, e quindi le nazioni, che hanno il dato
#...population_above_poverty_line non nullo!

country_lent_borr  = pd.merge(country_lent_borr, country_stats_not_null[["country_code", "population_above_poverty_line"]], on="country_code")


# In[69]:


#Calcolo il ratio (sopra linea di povertà), inserendolo in una colonna apposita

country_lent_borr["ratio_above_poverty"] = country_lent_borr["difference"]/country_lent_borr["population_above_poverty_line"]


# In[70]:


#Trovo la riga che corrisponde al massimo valore di ratio_above_poverty

country_lent_borr[country_lent_borr['ratio_above_poverty']==country_lent_borr['ratio_above_poverty'].max()]


# ### 12. For each year, compute the total amount of loans. Each loan that has planned expiration time and disburse time in different years must have its amount distributed proportionally to the number of days in each year. 
# 

# In[71]:


#Resetto l'indice

loans_not_null = loans_not_null.reset_index()


# In[72]:


#Rimuovo il fuso orario e le ore per facilitare i calcoli successivi tra date ed evitare errori

loans_not_null["disburse_time"] = loans_not_null["disburse_time"].dt.tz_localize(None)
loans_not_null["planned_expiration_time"] = loans_not_null["planned_expiration_time"].dt.tz_localize(None)


# In[73]:


loans_not_null["disburse_time"] = loans_not_null["disburse_time"].dt.normalize()
loans_not_null["planned_expiration_time"] = loans_not_null["planned_expiration_time"].dt.normalize()


# ***

# In[74]:


#Divido loans in loans_same_year (disburse e expiration hanno lo stesso anno) e loans_diff_year

loans_same_year= loans_not_null[loans_not_null["disburse_time"].dt.year == loans_not_null["planned_expiration_time"].dt.year][["loan_id" , "disburse_time", "planned_expiration_time","loan_amount"]]
loans_diff_year = loans_not_null[loans_not_null["disburse_time"].dt.year != loans_not_null["planned_expiration_time"].dt.year][["loan_id" ,"disburse_time", "planned_expiration_time","loan_amount"]]


# In[75]:


#Un modo di procedere è quello di creare due funzioni, una per loans_same_year e una per loans_diff_year che calcolino l'ammontare della loan relativa ad ogni anno
#Si va poi a sommare tutti gli ammontare.

#Creo un dataframe vuoto che andrò a riempire successivamente, le cui colonne corrispondono agli anni. 
#Gli anni vanno da __ a ___ :

#years_min_max = [loans["planned_expiration_time"].max().year, loans["planned_expiration_time"].min().year,
#                 loans["disburse_time"].max().year, loans["disburse_time"].min().year]
#print(max(years_min_max), min(years_min_max))

#year_amount = pd.DataFrame(0, index=[0], columns=["2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018"])

#def loan_func_same_years(df):  
#    year_amount.iloc[0][df["disburse_time"].strftime("%Y")] += df["loan_amount"]

#def loan_func_diff_years(df):  
#    completed_loop=0
#    start_date = df.disburse_time
#    exp_date = df.planned_expiration_time
#    year = [df["disburse_time"].strftime("%Y")]
#    tot_days = (exp_date - start_date).days+1
#   year_amount[year] += (df.loan_amount*((datetime.datetime(start_date.year, 12, 31) - start_date).days+1) / (tot_days))
#    start_date = datetime.datetime(start_date.year+1, 1, 1)
#    year = [start_date.strftime("%Y")]
#    while completed_loop != 1:
#        if start_date.year == exp_date.year:
#                year_amount[year] += (df.loan_amount*(exp_date - start_date).days+1) / (tot_days)
#                completed_loop=1
#        else:
#                year_amount[year] += (df.loan_amount*(datetime.datetime(start_date.year, 12, 31) - start_date).days+1) / (tot_days)
#                start_date = datetime.datetime(start_date.year+1, 1, 1)
#                year = [start_date.strftime("%Y")]
    
# Applico le due funzioni rispettivamente, e stampo il risultato:

#loans_same_year.apply(loan_func_same_years, axis=1)
#loans_diff_year.apply(loan_func_diff_years, axis=1)
#print(year_amount)

#Questo metodo però è particolarmente dispensioso e lento, in quanto usa dei cicli particolarmente pesanti


# In[76]:


#Un modo migliore consiste nel vettorizzare (almeno in gran parte) il processo. 
#Divido loans in loans_same_year (disburse e expiration hanno lo stesso anno) e loans_diff_year

#Ora considero loans_diff_year
#Voglio quindi passare da questo:

#Loan_id	Loan_amount    Disburse_time	Planned_Expiration
#ID 1 	    5000           2015/11/12    	2017/3/4 

#a questo:

#Loan_id    Loan_amount 	Disburse_time  	Start_date	End_year_date	Planned_expiration	Days_to_end	    Tot_days
#ID 1	5000                2015/11/12  	2015/1/1	2015/12/31	    2017/3/4 	        days_to_end 	tot_days
#ID 1 	5000                2015/11/12  	2016/1/1  	2016/12/31	    2017/3/4   	        days_to_end 	tot_days
#ID 1	5000                2015/11/12  	2017/1/1  	2017/12/31	    2017/3/4   	        Days_to_end	    tot_days

#In cui start_date e end_date sono delle colonne che mi servono per calcolare due dati importanti:
#1) days_to_end: sono i giorni di durata del loan divisi per anno di appartenenza
#2) tot_days: sono i giorni totali.
#In questo modo ho diviso i giorni totali nei vari anni di durata del loan. 

#Una soluzione di questo tipo si scontra con il principio di atomicità dei dati ma rende la soluzione del problema più veloce
#e meno dispendiosa dal punto di vista computazionale. Per questo motivo, questa va preferita all'altra soluzione.


# In[77]:


#Procedo ad applicare quanto scritto sopra


# In[78]:


#Ripeto le righe n=(anni di durata+1) volte, seguendo la logica di cui sopra

loans_diff_year = loans_diff_year.loc[loans_diff_year.index.repeat(loans_diff_year.planned_expiration_time.dt.year - loans_diff_year.disburse_time.dt.year + 1)]


# In[79]:


#Creo la colonna start_time

loans_diff_year["start_time"] = loans_diff_year.drop_duplicates()["disburse_time"].dt.year.apply(lambda x: datetime.datetime(x, 1, 1)) 


# In[80]:


#Vado a modificarla incrementando di un anno ogni volta che la riga si ripete, per ottenere il dataframe che mi serve 

y = loans_diff_year["disburse_time"].dt.year
loans_diff_year["start_time"] = pd.to_datetime(loans_diff_year.groupby(loans_diff_year["loan_id"]).cumcount() + y, format='%Y')


# In[81]:


#Creo end_time e la popolo similmente a start_time

loans_diff_year["end_time"] = pd.to_datetime(loans_diff_year.groupby(loans_diff_year["loan_id"]).cumcount() + y, format='%Y')


# In[82]:


loans_diff_year["end_time"] = loans_diff_year["start_time"].dt.year.apply(lambda x: datetime.datetime(x, 12, 31)) 


# In[83]:


#Ora voglio calcolare i days_to_end senza dover applicare una logica come la seguente. 

#def func(df):
#    if df["planned_expiration_time"].year > df["end_time"].year:  
#        if df["disburse_time"].year == df["start_time"].year:
#            df["days_to_end"] = (df["end_time"] - df["disburse_time"]).days
#        else: 
#            df["days_to_end"] = (df["end_time"] - df["start_time"]).days
#    elif df["planned_expiration_time"].year == df["end_time"].year:
#        df["days_to_end"] = (df["planned_expiration_time"] - df["start_time"]).days

#Posso fare così:

#df.loc[condizioni, "days_to_end"] = assegno valore


# In[84]:


#Prima resetto l'indice

loans_diff_year = loans_diff_year.reset_index()


# In[85]:


#Implemento la logica che ho descritto poco sopra

loans_diff_year.loc[(loans_diff_year["planned_expiration_time"].dt.year > loans_diff_year["end_time"].dt.year) & (loans_diff_year["disburse_time"].dt.year == loans_diff_year["start_time"].dt.year), "days_to_end"] = (loans_diff_year["end_time"] - loans_diff_year["disburse_time"]).dt.days +1                                                


# In[86]:


#Implemento la logica che ho descritto poco sopra

loans_diff_year.loc[(loans_diff_year["planned_expiration_time"].dt.year > loans_diff_year["end_time"].dt.year) & (loans_diff_year["disburse_time"].dt.year != loans_diff_year["start_time"].dt.year), "days_to_end"] = (loans_diff_year["end_time"] - loans_diff_year["start_time"]).dt.days +1                                           


# In[87]:


#Implemento la logica che ho descritto poco sopra

loans_diff_year.loc[(loans_diff_year["planned_expiration_time"].dt.year == loans_diff_year["end_time"].dt.year), "days_to_end"] = (loans_diff_year["planned_expiration_time"] - loans_diff_year["start_time"]).dt.days +1    


# In[88]:


#display(loans_diff_year.head())


# In[89]:


#Calcolo tot_days

loans_diff_year["tot_days"] = (loans_diff_year["planned_expiration_time"] - loans_diff_year["disburse_time"]).dt.days +1            


# In[90]:


#Calcolo loan_amount_per_year secondo l'equazione presente nel punto 12 del progetto)

loans_diff_year["loan_amount_per_year"] = (loans_diff_year["loan_amount"]* loans_diff_year["days_to_end"])/loans_diff_year["tot_days"]


# In[91]:


#Creo un nuovo dataframe sommando le somme di loan_amount_per_year raggruppandole per l'anno di appartenenza

loans_diff_year_tot = loans_diff_year.groupby(loans_diff_year["start_time"].dt.year)["loan_amount_per_year"].sum().to_frame().reset_index()
display(loans_diff_year_tot)


# In[92]:


#Creo un nuovo dataframe sommando le somme di loan_amount_per_year_same raggruppandole per l'anno di appartenenza
#Come prima ma per loans_same_year

loans_same_year_tot = loans_same_year.groupby(loans_same_year["disburse_time"].dt.year)["loan_amount"].sum().to_frame().reset_index()
display(loans_same_year_tot)


# In[93]:


#Faccio una merge dei due dataframe 

tot = pd.merge(loans_diff_year_tot, loans_same_year_tot, left_on ="start_time", right_on="disburse_time", how="outer")


# In[94]:


display(tot)


# In[95]:


#Riempio i valori vuoti con 0, in quanto potrebbero creare problemi
#Se in una riga abbiamo "X" = 50 e "Y =  NaN", la loro somma ci darebbe NaN
#Noi invece vogliamo che la loro somma X+Y sia = 50. 
#Inoltre bisogna sistemare le colonne start_time e disburse_time, per le righe a cui corrisponde un anno che era presente solo 
#in uno dei due dataframe a cui ho fatto la merge (riga corrispondente all'anno: 2011)

tot[["loan_amount_per_year" , "loan_amount"]] = tot[["loan_amount_per_year" , "loan_amount"]].fillna(0)
tot["start_time"] = tot["start_time"].fillna(tot["disburse_time"])
tot["disburse_time"] = tot["disburse_time"].fillna(tot["start_time"])


# In[96]:


display(tot)


# In[97]:


#Sommo le due colonne degli ammontare dei due dataframe in una sola colonna

tot["total_per_year"] = tot["loan_amount_per_year"] + tot["loan_amount"]
tot = tot[["start_time", "total_per_year"]]


# In[98]:


display(tot)


# ***

# OS: Windows 10 <br>
# CPU: Intel Core i5-6600 3.30GHz <br>
# RAM: 16GB <br>
# GPU: Nvidia GeForce GTX 1060 6gb <br>
# Environment: Jupyter Notebook <br>
# <br>
# Tempo totale di esecuzione sulla macchina = 2 min, 38 sec

# ***
