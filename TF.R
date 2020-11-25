install.packages("readxl")
library(readxl)

file.choose()

ruta_excel<-"D:\\Documentos\\Cienciasdelacomputación\\2020-02\\Administración de datos\\Projects\\PreviaTrabajo\\Partos atendidos por especialistas.xlsx"

datos<- read_excel(ruta_excel, sheet='serie-partos CU-2.8', range = 'B18:O45')

View(datos)
names(datos)[2]="Año 2000"
names(datos)[3]="Año 2004/2006"
names(datos)[4]="Año 2007/2008"
names(datos)[5]="Año 2009"
names(datos)[6]="Año 2010"
names(datos)[7]="Año 2011"
names(datos)[8]="Año 2012"
names(datos)[9]="Año 2013"
names(datos)[10]="Año 2014"
names(datos)[11]="Año 2015"
names(datos)[12]="Año 2016"
names(datos)[13]="Año 2017"
names(datos)[14]="Año 2018"
########Preparación de los datos##########
#MUESTREO-Se tomará como muestra los datos de cada region desde el año 2014 hasta el 20018
str(datos)

datosMuestra<-datos[c(1:nrow(datos)),c(1,8:14)]
View(datosMuestra)
#NORMALIZACION
names(datosMuestra)[2]="2012"
names(datosMuestra)[3]="2013"
names(datosMuestra)[4]="2014"
names(datosMuestra)[5]="2015"
names(datosMuestra)[6]="2016"
names(datosMuestra)[7]="2017"
names(datosMuestra)[8]="2018"
#IMPUTACION Y ELIMINACION DE VALORES ANOMALOS
for(i in 1:nrow(datosMuestra)){
  for(j in 1:ncol(datosMuestra)){
    datosMuestra[i,j][datosMuestra[i,j]=="-"]<-NA
  }
}
########################################################################################################






file.choose()

ruta_excel2<-"D:\\Documentos\\Cienciasdelacomputación\\2020-02\\Administración de datos\\Projects\\PreviaTrabajo\\Adolescentes embarazadas.xlsx"

datos2<- read_excel(ruta_excel2, sheet='mater adoles 2.10', range = 'B12:P39')

View(datos2)
names(datos2)[2]="Año 2000"
names(datos2)[3]="Año 2004/2006"
names(datos2)[4]="Año 2007/2008"
names(datos2)[5]="Año 2009"
names(datos2)[6]="Año 2010"
names(datos2)[7]="Año 2011"
names(datos2)[8]="Año 2012"
names(datos2)[9]="Año 2013"
names(datos2)[10]="Año 2014"
names(datos2)[11]="Año 2015"
names(datos2)[12]="Año 2016"
names(datos2)[13]="Año 2017"
names(datos2)[14]="Año 2018"
########Preparación de los datos##########
#MUESTREO-Se tomará como muestra los datos de cada region desde el año 2014 hasta el 20018
str(datos2)

datosMuestra2<-datos2[c(1:nrow(datos2)),c(1,8:14)]
View(datosMuestra2)
#NORMALIZACION
names(datosMuestra2)[2]="2012"
names(datosMuestra2)[3]="2013"
names(datosMuestra2)[4]="2014"
names(datosMuestra2)[5]="2015"
names(datosMuestra2)[6]="2016"
names(datosMuestra2)[7]="2017"
names(datosMuestra2)[8]="2018"
#IMPUTACION Y ELIMINACION DE VALORES ANOMALOS
for(i in 1:nrow(datosMuestra2)){
  for(j in 1:ncol(datosMuestra2)){
    datosMuestra2[i,j][datosMuestra2[i,j]=="-"]<-NA
  }
}
str(datosMuestra2)
datosMuestra2$"2012"<-as.numeric(datosMuestra2$"2012")
datosMuestra2$"2013"<-as.numeric(datosMuestra2$"2013")
datosMuestra2$"2014"<-as.numeric(datosMuestra2$"2014")




########################################################################################################

########Modelado de datos estruturados##########
library(RMySQL)
library(DBI)
library(dplyr)

driver=MySQL()
host="35.172.128.83"
port=3306
user="admininfo"
password="admininfo"
dbname="partosadolescentes"

if(dbCanConnect(drv=driver,port=port,user=user,host=host,password=password,dbname=dbname))#Para ver si se puede conectar con la base de datos
{
  conexion<-dbConnect(drv=driver,port=port,user=user,host=host,
                      password=password,
                      dbname=dbname) 
}
View(conexion)
dbIsValid(conexion)
View(dbListTables(conexion))

#MODELADO E IMPLEMENTACION

#Id para todos
id<-c(1:nrow(datosMuestra))

#Dataframe para la tabla departamentos
nombre<-list()
nombre<-c(1)
nombre<-c(nombre,datosMuestra$Departamento)
nombre<-gsub(c("á"), "a", nombre)
nombre<-gsub(c("Á"), "A", nombre)
nombre<-gsub(c("é"), "e", nombre)
nombre<-gsub(c("É"), "E", nombre)
nombre<-gsub(c("í"), "i", nombre)
nombre<-gsub(c("Í"), "I", nombre)
nombre<-gsub(c("ó"), "o", nombre)
nombre<-gsub(c("Ó"), "O", nombre)
nombre<-gsub(c("ú"), "u", nombre)
nombre<-gsub(c("Ú"), "U", nombre)
nombre<-nombre[-c(1)]

dfdepartamento<-data.frame(id,nombre)
View(dfdepartamento)

#Creando la tabla departamento con datos
query<-sqlCreateTable(conexion,"departamentos",
                      dfdepartamento)
query
#Integramos la tabla departamento a la base de datos
dbExecute(conexion, statement=query)


#Dataframe para la tabla fechas
library(lubridate)
library(zoo)
id<-c(1:7)
fecha<-c("2012","2013","2014","2015","2016","2017","2018")
dffecha<-data.frame(id,fecha)

#Creando la tabla fecha con datos
query<-sqlCreateTable(conexion,"fechas",
                      dffecha)
query
#Integramos la tabla fecha a la base de datos
dbExecute(conexion, statement=query)

#Dataframe para la tabla Profesionales
id<-c(1:(nrow(datosMuestra)*((ncol(datosMuestra)-1))))
id

#Creando listas vacias necesarias

departamento_id<-list()
fecha_id<-list()
porcentaje<-list()
#creando elemento guia en la lista
departamento_id<-c(1)
#llenando datos en la lista departamento_id
for (i in 1:7) {
  departamento_id<-c(departamento_id,dfdepartamento$id)
}
#Eliminar el elemnto guia, es el primero elemento
departamento_id<-departamento_id[-c(1)]

#Lenando datos en la lista porcentaje
porcentaje<-c(datosMuestra$"2012",datosMuestra$"2013",datosMuestra$"2014",
              datosMuestra$"2015",datosMuestra$"2016",datosMuestra$"2017",
              datosMuestra$"2018")
#Los datos obtenidos son cadena de caracteres hay que convertirlos a numericos
porcentaje<-as.numeric(porcentaje)

#Creando elemento guia en la lista fecha
fecha_id<-c(1)

#Llenando datos en la lista fecha_id

for (i in 1:7) {
  for (j in 1:nrow(datosMuestra)) {
    fecha_id<-c(fecha_id,i)
  }
}

#Eliminar el elemnto guia, es el primero elemento
fecha_id<-fecha_id[-c(1)]
#Creando la el data frame profesionales
dfprofesionales<-data.frame(id,departamento_id,porcentaje,fecha_id)
View(dfprofesionales)

#Creando la tabla años con datos
query<-sqlCreateTable(conexion,"porcentaje_profesionales",
                      fields=c(id="bigint",departamento_id="bigint",porcentaje="double",fecha_id="bigint"))
query
#Integramos la tabla años a la base de datos
dbExecute(conexion, statement=query)



dbWriteTable(conexion,name="porcentaje_profesionales", value=dfprofesionales, overwrite=TRUE)
dbWriteTable(conexion,name="departamentos", value=dfdepartamento, overwrite=TRUE)
dbWriteTable(conexion,name="fechas", value=dffecha, overwrite=TRUE)




######################################################################################################





#Dataframe para la tabla adolescentes
id<-c(1:(nrow(datosMuestra2)*((ncol(datosMuestra2)-1))))
id

#Creando listas vacias necesarias

departamento_id<-list()
fecha_id<-list()
porcentaje<-list()
#creando elemento guia en la lista
departamento_id<-c(1)
#llenando datos en la lista departamento_id
for (i in 1:7) {
  departamento_id<-c(departamento_id,dfdepartamento$id)
}
#Eliminar el elemnto guia, es el primero elemento
departamento_id<-departamento_id[-c(1)]

#Llenando datos en la lista porcentaje
porcentaje<-c(datosMuestra2$"2012",datosMuestra2$"2013",datosMuestra2$"2014",
              datosMuestra2$"2015",datosMuestra2$"2016",datosMuestra2$"2017",
              datosMuestra2$"2018")
#Los datos obtenidos son cadena de caracteres hay que convertirlos a numericos
porcentaje<-as.numeric(porcentaje)

#Creando elemento guia en la lista fecha
fecha_id<-c(1)

#Llenando datos en la lista fecha_id

for (i in 1:7) {
  for (j in 1:nrow(datosMuestra)) {
    fecha_id<-c(fecha_id,i)
  }
}

#Eliminar el elemnto guia, es el primero elemento
fecha_id<-fecha_id[-c(1)]
#Creando la el data frame profesionales
dfadolescentes<-data.frame(id,departamento_id,porcentaje,fecha_id)
View(dfadolescentes)

#Creando la tabla años con datos
query<-sqlCreateTable(conexion,"porcentaje_adolescentes_emb",
                      fields=c(id="bigint",departamento_id="bigint",porcentaje="double",fecha_id="bigint"))
query
#Integramos la tabla años a la base de datos
dbExecute(conexion, statement=query)



dbWriteTable(conexion,name="porcentaje_adolescentes_emb", value=dfadolescentes, overwrite=TRUE)
#dbWriteTable(conexion,name="departamentos", value=dfdepartamento, overwrite=TRUE)
#dbWriteTable(conexion,name="fechas", value=dffecha, overwrite=TRUE)




###########################################################################################################





########Transformacion y consultas exploratorias##########
#1. Seleccionar elementos de tabla departamentos
qdepartamentos<-dbGetQuery(conexion,"select * from departamentos")
View(qdepartamentos)

#2. Seleccionar elementos de la columna fecha de la tabla fechas
qfechas_fecha<-dbGetQuery(conexion,"SELECT fecha FROM partosadolescentes.fechas")
View(qfechas_fecha)

#3.	Consultar la division o descartar datos de la tabla profesionales que tengan datos en el Departamento de Amazonas(id=1)
qdivision_profesionales_dep<-dbGetQuery(conexion,"SELECT * from partosadolescentes.porcentaje_profesionales pp
                                                    Where NOT EXISTS(
                                                    select * 
                                                    from partosadolescentes.departamentos as d
                                                    where d.nombre = 'Amazonas'
                                                    and pp.departamento_id=d.id)")
View(qdivision_profesionales_dep)
#4.	Consultar la division o descartar datos de la tabla profesionales que tengan datos el 2012(id=1)
qdivision_profesionales_fecha<-dbGetQuery(conexion,"SELECT * from partosadolescentes.porcentaje_profesionales pp
                                                    Where NOT EXISTS(
                                                    select * 
                                                    from partosadolescentes.fechas as f
                                                    where f.fecha = '2012'
                                                    and pp.fecha_id=f.id)")
View(qdivision_profesionales_fecha)

#5.	Consultar el filtrado de datos en tabla procentaje_profesionales por fecha 2012   ............
qfiltrado_fecha<-dbGetQuery(conexion,"select departamento_id, porcentaje, fecha_id
                                from partosadolescentes.porcentaje_profesionales as pp 
                                where pp.fecha_id='1'")
View(qfiltrado_fecha)

#6 Consultar el filtrado de datos en tabla porcentaje_profesionales donde los porcentaje sean nulos
qfiltrado_porcentaje<-dbGetQuery(conexion,"select departamento_id, porcentaje 
                                 from partosadolescentes.porcentaje_profesionales as pp 
                                 where pp.porcentaje is null")
View(qfiltrado_porcentaje)


#7 consultar el Join de datos en la tabla porcentaje para saber el departamento donde se encuentra ese porcentaje
qjoin_porcentaje_dep<-dbGetQuery(conexion,"select pp.id as id_porcentaje_profesional,
                                       pp.porcentaje,
                                       pd.id as id_departamento,
                                       pd.nombre as departamento 
                                       from partosadolescentes.porcentaje_profesionales as pp 
                                       join partosadolescentes.departamentos pd
                                       on pp.departamento_id = pd.id")
View(qjoin_porcentaje_dep)


#8 consultar el Join de datos en la tabla porcentaje para saber la fecha(año) cuando se realizó ese porcentjae
qjoin_porcentaje_fec<-dbGetQuery(conexion,"select pp.id as id_porcentaje_profesional,
                                       pp.porcentaje,
                                       pf.id as id_fecha,
                                       pf.fecha as año 
                                       from partosadolescentes.porcentaje_profesionales as pp 
                                       join partosadolescentes.fechas pf
                                       on pp.fecha_id = pf.id")
View(qjoin_porcentaje_fec)


#9 Seleccionamos toda la tabla de porcentaje_profesionales
df_tabla_profesionales<-dbGetQuery(conexion,"select * from partosadolescentes.porcentaje_profesionales")
View(df_tabla_profesionales)

#10 Seleccionamos toda la tabla de porcentaje_emb_juv
df_tabla_porcentaje_adolescentes_emb<-dbGetQuery(conexion,"select * from partosadolescentes.porcentaje_adolescentes_emb")
View(df_tabla_porcentaje_adolescentes_emb)

#11 consultar el Join de datos en la tabla porcentaje para saber el departamento donde se realizó ese porcentaje
df_prof_ados_dep<-dbGetQuery(conexion,"select 
                                       pd.nombre as Departamento,
                                       pp.id as id_porcentaje_profesional,
                                       pp.porcentaje as porcentaje_profesional,
                                       pa.id as id_porcentaje_adolescente,
                                       pa.porcentaje as porcentaje_adolescente
                                       from partosadolescentes.porcentaje_profesionales as pp 
                                       join partosadolescentes.porcentaje_adolescentes_emb as pa
                                       join partosadolescentes.departamentos pd
                                       on pp.id = pa.id and pp.departamento_id=pd.id")
View(df_prof_ados_dep)

#12 consultar el Join de datos en la tabla porcentaje para saber el departamento por cada fecha(año) cuando se realizó ese porcentjae
df_prof_fec_dep<-dbGetQuery(conexion,"select 
                                       pd.nombre as Departamento,
                                       pf.fecha as Año,
                                       pp.porcentaje as porcentaje_profesional
                                       from partosadolescentes.porcentaje_profesionales as pp 
                                       join partosadolescentes.fechas pf
                                       join partosadolescentes.departamentos pd
                                       on pp.fecha_id = pf.id and pp.departamento_id=pd.id")
View(df_prof_fec_dep)
#13.	Consultar el filtrado de datos en tabla procentaje_profesionales por fecha 2012,amazonas y ayacucho   ............
df_prof_depar_nom_12<-dbGetQuery(conexion,"select pd.nombre as Departamento, porcentaje, fecha_id
                                from partosadolescentes.porcentaje_profesionales as pp
                                join partosadolescentes.departamentos as pd
                                on pd.id=pp.departamento_id 
                                where pp.fecha_id='1' and pd.id < 5")
View(df_prof_depar_nom_12)

#14 consultar el Join de datos en la tabla porcentaje para saber el departamento por cada fecha(año) cuando se realizó ese porcentjae
df_prof_fec_dep_4p<-dbGetQuery(conexion,"select 
                                       pd.nombre as Departamento,
                                       pf.fecha as Año,
                                       pp.porcentaje as porcentaje_profesional
                                       from partosadolescentes.porcentaje_profesionales as pp 
                                       join partosadolescentes.fechas pf
                                       join partosadolescentes.departamentos pd
                                       on pp.fecha_id = pf.id and pp.departamento_id=pd.id
                                       where pd.id < 5")
View(df_prof_fec_dep_4p)

#Primero reemplazamos los datos de la columna porcentajes que son NA por 0
df_tabla_profesionales$porcentaje[is.na(df_tabla_profesionales$porcentaje)] <- 0
#MEDIA aritmetica del procentaje de profesionales
mean(df_tabla_profesionales$porcentaje)

#MAXIMO del porcentaje de profesionales
max(df_tabla_profesionales$porcentaje)

#MINIMO del porcentaje de profesionales
min(df_tabla_profesionales$porcentaje)

#CUARTILES del porcentaje de profesionales
quantile(df_tabla_profesionales$porcentaje, prob=c(0,0.25,0.5,0.75,1))

#PERCENTILES del porcentaje de profesionales TODOS
quantile(df_tabla_profesionales$porcentaje, prob=seq(0, 1, length = 101))


#####################5-EXPLORACION VISUAL DE DATOS#####################

library (ggplot2)
#Diagrama de dispersion adolescentes vs profecionales
plot(df_prof_ados_dep)
plot(df_prof_ados_dep$porcentaje_adolescente,df_prof_ados_dep$porcentaje_profesional)

#Diagrama de barras
ggplot(data=qjoin_porcentaje_dep, aes(x=qjoin_porcentaje_dep$departamento, y=qjoin_porcentaje_dep$porcentaje)) + 
  geom_bar(stat="identity", position="stack")+coord_flip()+labs(x= "Departamentos", y = "Porcentaje Profesionales")

ggplot(df_prof_fec_dep, aes(df_prof_fec_dep$Departamento, df_prof_fec_dep$porcentaje_profesional, fill = df_prof_fec_dep$Año)) +
  geom_bar(stat = "identity", position = "dodge") + coord_flip() + labs(x= "Departamentos", y = "Porcentaje Profesionales", fill="Año")

#Diagrama de cajas
boxplot(formula = df_prof_fec_dep$porcentaje_profesional ~ df_prof_fec_dep$Año, data =  df_prof_fec_dep,  xlab = "Año", ylab = "Porcentaje de profesionales", 
        col = c("orange3", "yellow3", "green3", "grey","red3","blue3","brown3"))
boxplot(formula = df_prof_fec_dep_4p$porcentaje_profesional ~ df_prof_fec_dep_4p$Departamento, data =  df_prof_depar_nom_12,  xlab = "Departamento", ylab = "Porcentaje de profesionales")

#Diagramas de serie de tiempo
