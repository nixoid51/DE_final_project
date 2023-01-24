# DE_final_project
____
# Итоговый проект по курсу "Инженер данных"

- Тема: Проект 3
- Выполнила: Лазарева Ирина Михайловна

## Цель: создать скрипт для формирования витрины на основе логов web-сайта
____

# Решение поставленной задачи:

## 0. Формирование нового файла данных с информацией об устройстве и браузеру
на основе анализа User_agent (https://pypi.org/project/user-agents/)
```
import csv
from csv import reader

new_file = open('access.log.m','w+')
csv_writer = csv.writer(new_file,delimiter=' ')
with open('access.log','r') as file:
    csv_reader = reader(file,delimiter=' ')
    for line in csv_reader:
        ua_string = line[9]
        user_agent = parse(ua_string)
        line.append(user_agent.browser.family)
        line.append(user_agent.device.brand)
        csv_writer.writerow(line)
```

## 1. Создаем схему для хранения "сырых" данных:
```
schema = T.StructType(fields=[
    T.StructField("IP", T.StringType(), True),
    T.StructField("sign_1", T.StringType(), True),
    T.StructField("sign_2", T.StringType(), True),
    T.StructField("Date_access", T.StringType(), True),
    T.StructField("Date_access_", T.StringType(), True),
    T.StructField("Action", T.StringType (), True),
    T.StructField("Status", T.IntegerType (), True),
    T.StructField("Size", T.IntegerType (), True),
    T.StructField("sign_3", T.StringType (), True),
    T.StructField("User_agent", T.StringType (), True),
    T.StructField("sign_4", T.StringType (), True),
    
    T.StructField("Browser", T.StringType(), True),
    T.StructField("Device", T.StringType(), True),
])
```

## 2. Создаем датафрейм на основе схемы и данных файла access.log.m:
```
   df = spark.read.csv("access.log.m", schema=schema, sep=" ")
```

## 3. Проверка данных на корректность:
 Был использован контроль значений каждого столбца на наличие содержимого.
 Пример для столбца со значениями IP пользователя
```
   df.count()
   df.groupby("IP")\
      .agg(F.count("*").alias("activ"))\
      .orderBy("activ", ascending = False)\
      .show()
   df.groupby("IP")\
      .agg(F.count("*").alias("activ"))\
      .orderBy("activ", ascending = False)\
      .agg(F.sum("activ"))\
      .show()
``` 

## 4.Убираем неинформативные столбцы. Получаем базовую таблицу
 Неинформативные столбцы - столбцы не содержащие полезной информации
```
   df_Base = df.drop("sign_1", "sign_2", "Date_access_", "sign_3", "sign_4")
```

## 5. Заменяем нулевые значения дата-фрейма на "other"
```
   df_Base = df_Base.na.fill("other")
```

## 6. Формирование таблицы_1 "Устройства по пользователям"
```
   df_Devices = df_Base.groupby('Device')\
             .agg(F.count('IP').alias('Count_Users'))\
             .withColumn('Count_Users',F.col('Count_Users'))\
             .orderBy('Count_Users', ascending = False)
             
   df_Devices_Users = df_Devices.withColumn('All_Users', F.sum('Count_Users').over(Window.partitionBy()))\
            .withColumn('Ratio_Users', F.round(F.col('Count_Users') / F.col('All_Users'), 5))\
            .select('Device', 'Count_Users', 'Ratio_Users')
       
   df_Devices_Users.show()
```

## 7. Формирование таблицы_2 "Устройства по действиям пользователей"
```
   df_Devices_ = df_Base.groupby('Device')\
             .agg(F.count('Action').alias('Count_Actions'))\
             .withColumn('Count_Actions',F.col('Count_Actions'))\
             .orderBy('Count_Actions', ascending = False)
             
   df_Devices_Actions = df_Devices_.withColumn('All_Actions', F.sum('Count_Actions').over(Window.partitionBy()))\
            .withColumn('Ratio_Actions', F.round(F.col('Count_Actions') / F.col('All_Actions'), 5))\
            .select('Device', 'Count_Actions', 'Ratio_Actions')
       
   df_Devices_Actions.show()
```

## 8. Соединение таблиц 1 и 2 и создание суррогатного ключа
```
   df_Devices_About = df_Devices_Users.join(df_Devices_Actions, ["Device"])
   
   df_Devices_About = df_Devices_About.withColumn('original_order', F.monotonically_increasing_id())
   df_Devices_About = df_Devices_About.withColumn('id_device', F.row_number().over(Window.orderBy('original_order')))
   df_Devices_About = df_Devices_About.drop('original_order')
   
   df_Devices_About.show()
```

## 9. Формирование таблицы 3 "Браузеры"
```
   df_Devices_Browsers = df_Base.groupby('Device','Browser')\
                             .agg(F.count('Browser').alias('Count_Browser'))
   
   window = Window.partitionBy(df_Devices_Browsers['Device'])\
               .orderBy(df_Devices_Browsers['Count_Browser'].desc())
   df_Browsers = df_Devices_Browsers.select('*', F.row_number().over(window).alias('row_number')).where(F.col('row_number') <= 5)\
             .select('Device', 'Browser', 'Count_Browser', 'row_number')\
             .orderBy(df_Devices_Browsers['Device'],df_Devices_Browsers['Count_Browser'].desc())
   
   df_Browsers.show()
```

## 10. Формирование таблицы 4 "Ответы сервера"
```
   df_Status_200 = df_Base.filter(df_Base.Status != "200")
   
   df_Status = df_Status_200.groupby('Device')\
             .agg(F.count('Status').alias('Count_Status_all'))\
             .withColumn('Count_Status_all',F.col('Count_Status_all'))
   
   df_Status_Answers = df_Status_200.groupby('Device','Status')\
             .agg(F.count('Status').alias('Count_Status'))\
             .withColumn('Count_Status',F.col('Count_Status'))\
             .orderBy('Device', 'Count_Status', ascending = False)\
             .select('Device', 'Status', 'Count_Status')
   df_Server_Answers = df_Status.join(df_Status_Answers, ["Device"], "left")\
                    .orderBy('Device', 'Count_Status', ascending = True)
   
   df_Server_Answers.show()
```

## 11. Сохранение результирующих таблиц в файлы
```
   df_Devices_About.toPandas().to_csv('devices_about.csv',mode='w+',header=True,index=False)
   df_Browsers.toPandas().to_csv('browsers.csv',mode='w+',header=True,index=False)
   df_Server_Answers.toPandas().to_csv('server_answers.csv',mode='w+',header=True,index=False)
```
