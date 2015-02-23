# csvstorage
Хранение данных в csv-файлах с возможностью манипулировать ими с помощью некого подобия ActiveRecord

## Примеры использования

### Создание схемы данных для таблицы
```go
id := csvstorage.CSVField{"id", csvstorage.CSVFieldTypeInt}
title := csvstorage.CSVField{"title", csvstorage.CSVFieldTypeText}
csvstorage.CreateCSVTableSchema("table", id, title)
```
В результате будут созданы два файла `table.csv` и `table.schema` для хранения данных и схемы данных соответственно

table.schema:
```
id,int
title,text
```

### Вставка новых записей в таблицу

```go

// CSVValueRecord == map[string]string
r1 := make(csvstorage.CSVValueRecord)
r1["id"] = "1"
r1["title"] = "one"

r2 := make(csvstorage.CSVValueRecord)
r2["id"] = "2"
r2["title"] = "two"
csvstorage.Insert("table").Values(r1, r2).Do()
```
table.csv:
```
1,one
2,two

```

### Простая выборка

```go
q := csvstorage.Select("id", "title").From("table")
fmt.Println(q.Do())
```
Результат
```
[map[id:1 title:one] map[id:2 title:two]]
```


### Создание условий для фильтрации
Условия могут быть простыми и составными. Простые условия лишь проверяют данные на равенство. Составные содержат логический оператор (AND/OR) и два условия, которые погут быть как простыми, так и составными

Пример простого условия
```go
simpleWhere := csvstorage.NewSimpleWhere("id", "=", "1")
q := csvstorage.Select("id", "title").From("table").Where(simpleWhere)
fmt.Println(q.Do()
```
Результат
```
[map[id:1 title:one]]
```
Пример составного условия, содержащего два простых условия
```go
simpleWhere1 := csvstorage.NewSimpleWhere("id", "=", "1")
simpleWhere2 := csvstorage.NewSimpleWhere("title", "=", "two")
complexWhere := csvstorage.NewComplexWhereCondition("OR", simpleWhere1, simpleWhere2) // также можно использовать логический оператор "AND"

q := csvstorage.Select("id", "title").From("table").Where(complexWhere)
fmt.Println(q.Do())
```
Результат
```
[map[id:1 title:one] map[id:2 title:two]]
```

Кроме случая выборки, условия можно использовать при удалении и измении данных (см.ниже)

### Изменение записей

! Если при выполнении изменения данных не указано условие, то изменения применятся ко всем записям в таблице !

```go
simpleWhere := csvstorage.NewSimpleWhere("id", "=", "1")
r1 := make(csvstorage.CSVValueRecord)
r1["title"] = "~1~"
csvstorage.Update("table").Set(r1).Where(simpleWhere).Do()
```

table.csv до изменения
```
1,one
2,two
```
table.csv после изменения
```
1,~1~
2,two

```

### Удаление записей

! Если при выполнении удаления данных не указано условие, то удалятся все записи в таблице !

```go
simpleWhere := csvstorage.NewSimpleWhere("id", "=", "1")
csvstorage.Delete("table").Where(simpleWhere).Do()
```

table.csv до изменения
```
1,~1~
2,two
```
table.csv после изменения
```
2,two
```