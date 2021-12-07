import pandas as pd
from datetime import date, datetime

#pegar ruta del documento
Datos = pd.read_csv('C:\\Users\\marce\\Documents\\Proyecto gp 3.1\\captaciones\\CaptacionesAbril21.csv')

null = 'NULL'
Datos = Datos.fillna(null)
Squema = "public"
table = "Captaciones"
PrimarykEY = "ID_CAPTACIONES"
SkipColunms = []
Columns = []
count = 0
count2 = 0



SentenciaSQLTable1 = "CREATE TABLE " + Squema + "." + table + "( \n" + PrimarykEY + " SERIAL NOT NULL ,\n";
#Columns.append(PrimarykEY);
for i in list(Datos):
    valitator = False
    if(i==PrimarykEY):
        continue
    for j in SkipColunms:
        if(j==count):
            Columns.append(i);
            valitator =True
            break
    count += 1 
    if(valitator):
        continue
    SentenciaSQLTable1 += i.strip().replace(" ","_")+" TEXT, \n"
    Columns.append(i);
#########################################
SentenciaSQLTable1 += "CONSTRAINT pk_"+table+"_"+PrimarykEY+" PRIMARY KEY (" + PrimarykEY + ") \n);";

with open(table+'Tabla.txt','w') as f:
        f.write(SentenciaSQLTable1)
 #----------------------------------------------Datos-------------------------------------------------------------------------------       
SentenceInsert =""
for i in Datos.iloc():
    count = 1
    Insert1 = "INSERT INTO " + Squema + "." + table + "("
    Insert2="VALUES ("
    for j in Columns:
        Insert1+= j.strip().replace(" ","_")
        if((str(type(Datos.at[count2,j]))=="<class 'str'>") and count != 4):
            Insert2+= ("'"+str(Datos.at[count2,j])+"'")
        elif(count == 4):
            Fecha = Datos.at[count2,j]
            Partes = Fecha.split("/")
            nuevaFecha = Partes[2]+"-"+Partes[0]+"-"+Partes[1]
            Insert2+= ("'"+nuevaFecha+"'")
        else:
            Insert2+= (str(Datos.at[count2,j])) 
        if(len(Columns)==count):
            break
        Insert2+=", "
        Insert1+= ", "
        count+=1
    Insert1+=") "
    Insert2+="); "
    SentenceInsert+=Insert1+Insert2+"\n"
    count2+=1



with open(table+'Insert.txt','w') as file:
    file.write(SentenceInsert)
#print(SentenciaSQL)
