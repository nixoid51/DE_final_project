# DE_final_project
____
# Итоговый проект по курсу "Инженер данных"

- Тема: Проект 3
- Выполнила: Лазарева Ирина Михайловна

## Цель: создать скрипт для формирования витрины на основе логов web-сайта
____

# Решение поставленной задачи:

## 1. Создаем схему для хранения "сырых" данных:
![image](https://user-images.githubusercontent.com/49267469/210132939-d822a64c-a903-4698-8588-890d63ce33df.png)

## 2. Создаем датафрейм на основе схемы и данных файла access.log:
Для проверки решения сначала были использованы 100 первых строк логфайла
Для проверки решения сначала были использованы 100 первых строк логфайла
```path = "data/access.log"
   df1 = spark.read.csv(path, schema=schema, sep=" ")
```

## 3. Проверка данных на корректность:
 Был использован контроль значений каждого столбца на наличие содержимого
 Пример для столбца со значениями IP пользователя
```df1.count()
   df1.groupby("IP")\
      .agg(F.count("*").alias("activ"))\
      .orderBy("activ", ascending = False)\
      .show()
   df1.groupby("IP")\
      .agg(F.count("*").alias("activ"))\
      .orderBy("activ", ascending = False)\
      .agg(F.sum("activ"))\
      .show()
``` 
## 4.Убираем неинформативные столбцы:
 Неинформативные столбцы - столбцы не содержащие полезной информации
```df1_2 = df1.drop("sign_1", "sign_2", "Date_access_", "sign_3", "sign_4")
   df1_2.show()
```
 ## 5. Анализируем значения столбца User_agent для выделения списка данных по устройству и по браузеру
  Для парсинга строки User_agent используем библиотеку user_agents
  https://pypi.org/project/user-agents/
```from user_agents import parse
   n = df1_2.count()
   ls = [[],[]]
   for i in range(n):
        ua_string = str(df1_2.select(["User_agent"]).collect()[i])
        user_agent = parse(ua_string)
        ls.append([user_agent.browser.family, user_agent.device.brand])
```
## 6. Преобразовываем полученный список в датафрейм и соединяем его с основной таблицей:
  В результате - сформирована базовая таблица
```R = Row('Browser', 'Name_Device')
   df1_3 = spark.createDataFrame([R(x, y) for x, y in ls])
   df1_4 = df1_2.join(df1_3)
   df1_4.show()
```
## 7. Формирование таблицы_1 "Устройства по пользователям"
  Так как для дальнейшего анализа нужны данные по использованным устройствам, из результирующей 
  таблицы удаляются строки, в которых отсутствуют данные об устройствах
```df_Devices_ = df1_4.groupby('Name_Device')\
                      .agg(F.count('IP').alias('Count_Users'))\
                      .withColumn('Count_Users',F.col('Count_Users'))       
   df_Devices_.show()
   df_D2 = df_Devices_.filter(df_Devices_.Name_Device != "null")
   df_D2.show()
   df_Devices_Users = df_D2.withColumn('All_Users', F.sum('Count_Users').over(Window.partitionBy()))\
                            .withColumn('Ratio_Users', F.col('Count_Users') / F.col('All_Users'))\
                            .select('Name_Device', 'Count_Users', 'Ratio_Users')
       
   df_Devices_Users.show()
```
## 8. Формирование таблицы_2 "Устройства по действиям пользователей"
```df_D3 = df_Devices_Users.join(df1_4, df_Devices_Users.Name_Device == df1_4.Name_Device)\
                           .groupby(df1_4.Name_Device)\
                           .agg(F.count(df1_4.Action).alias('Count_Actions'))\
                           .withColumn('Count_Actions', F.col('Count_Actions'))
   df_D3.show() 
   df_Devices_Actions = df_D3.withColumn('All_Actions', F.sum('Count_Actions').over(Window.partitionBy()))\
                               .withColumn('Ratio_Actions', F.col('Count_Actions') / F.col('All_Actions'))\
                               .select('Name_Device', 'Count_Actions', 'Ratio_Actions')
       
   df_Devices_Actions.show()
```
## 9. Аналогично планируется получить таблицы 3 и 4
