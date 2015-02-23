package csvstorage

import (
	"encoding/csv"
	"os"
)

type SelectQuery struct {
	fields []string
	table  string
	where  WhereCondition
	limit  int
	schema CSVTableSchema
}

type SelectQueryResultRecord map[string]string

type SelectQueryResult []SelectQueryResultRecord

// инициализация запроса, в качестве аргументов указываются поля для выборки
func Select(fields ...string) *SelectQuery {
	q := new(SelectQuery)

	for _, v := range fields {
		q.fields = append(q.fields, v)
	}

	return q
}

// указание таблицы, из которой будет производиться выборка
func (this *SelectQuery) From(table string) *SelectQuery {
	r, _ := os.Open(table + ".schema")
	defer r.Close()

	this.schema = ReadSchema(r)

	this.table = table

	return this
}

// указание необходимых фильтров, с интерфейсом WhereCondition (см. csvwhere.go)
func (this *SelectQuery) Where(cond WhereCondition) *SelectQuery {
	this.where = cond

	return this
}

// максимальное количество записей в результате запроса
func (this *SelectQuery) Limit(limit int) *SelectQuery {
	this.limit = limit

	return this
}

// выполнение запроса
func (this *SelectQuery) Do() SelectQueryResult {
	r, _ := os.Open(this.table + ".csv")
	defer r.Close()
	csvReader := csv.NewReader(r)

	// записи из файла в формате .csv
	records, _ := csvReader.ReadAll()

	result := make(SelectQueryResult, 0)

	// какие поля будут в возвращаемой записи
	outputFields := make(map[string]int)
	for _, f := range this.fields {
		outputFields[f] = this.schema[f].index
	}
	// прохожу по всем записям
	for _, v := range records {
		// проверяю достигнут ли лимит записей
		if len(result) >= this.limit && this.limit != 0 {
			break
		}
		// создаю новую запись
		selectQueryResultRecord := make(SelectQueryResultRecord)
		// и заполняю ее значениями
		for fName, fIndex := range outputFields {
			selectQueryResultRecord[fName] = v[fIndex]
		}

		// затем проверяю, задано ли какое-то условие для фильтрации
		if this.where != nil {
			// если задано и оно проверка проходит удачно, то добавляю к результатам текущую запись
			if this.where.Compute(v, this.schema) {
				result = append(result, selectQueryResultRecord)
			}
		} else {
			// если фильтрация не задана, то просто добавляю запись
			result = append(result, selectQueryResultRecord)
		}

	}

	return result
}
