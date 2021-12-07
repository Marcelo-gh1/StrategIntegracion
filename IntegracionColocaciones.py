import pandas as pd
from datetime import date, datetime
size = 2000000
chunk = pd.read_csv('C:\\Users\\marce\\Documents\\Proyecto gp 3.1\\Colocaciones\\Colocaciones 2016.csv', chunksize=size)
null = 'NULL'
#Datos = Datos.fillna(null)
Squema = "public"
table = "Captaciones"
nametxt = "S3 OCT 2021"
PrimarykEY = "ID_COLOCACIONES"
SkipColunms = [3]
Columns = []
count = 0
count2 = 0
header = True


SentenciaSQLTable1 = "CREATE TABLE " + Squema + "." + \
    table + "( \n" + PrimarykEY + " SERIAL NOT NULL ,\n"
file = open('C:\\Users\\marce\\OneDrive - Universidad Técnica Particular de Loja - UTPL\\Documentos\\Universidad\\Yo\\9NO CICLO\\'+table+nametxt+'Insert.txt', 'w')

SentenceInsert = ""
for Datos in chunk:
    Datos = Datos.fillna(null)
    count2 = 0
    if(header):
        for i in list(Datos):
            valitator = False
            if(i == PrimarykEY):
                continue
            for j in SkipColunms:
                if(j == count):
                    #Columns.append(i)
                    valitator = True
                    break
            count += 1
            if(valitator):
                continue
            SentenciaSQLTable1 += i.strip().replace(" ", "_")+" TEXT, \n"
            Columns.append(i)
            header = False
    for i in Datos.iloc():
        count = 1
        Insert1 = "INSERT INTO " + Squema + "." + table + "("
        Insert2 = "VALUES ("
        for j in Columns:
            Insert1 += j.strip().replace(" ", "_")
            if((str(type(Datos.at[count2, j])) == "<class 'str'>") and count > 3 and Datos.at[count2, j] != 'NULL'):
                Insert2 += ("'"+str(Datos.at[count2, j])+"'")
            else:
                Insert2 += (str(Datos.at[count2, j]))
            if(len(Columns) == count):
                break
            Insert2 += ", "
            Insert1 += ", "
            count += 1
        Insert1 += ") "
        Insert2 += "); "
        file.write(Insert1+Insert2+"\n")
        #SentenceInsert += Insert1+Insert2+"\n"
        count2 += 1
file.close()
SentenciaSQLTable1 += "CONSTRAINT pk_"+table+"_" + \
    PrimarykEY+" PRIMARY KEY (" + PrimarykEY + ") \n);" 


#with open(table+'Tabla.txt', 'w') as f:
   # f.write(SentenciaSQLTable1)


# print(SentenciaSQL)'''

