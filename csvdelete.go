package csvstorage

import (
	"encoding/csv"
	"os"
)

type DeleteQuery struct {
	table  string
	where  WhereCondition
	schema CSVTableSchema
}

func Delete(table string) *DeleteQuery {
	q := new(DeleteQuery)
	r, _ := os.Open(table + ".schema")
	defer r.Close()

	q.schema = ReadSchema(r)

	q.table = table

	return q
}

func (this *DeleteQuery) Where(where WhereCondition) *DeleteQuery {
	this.where = where

	return this
}

func (this *DeleteQuery) Do() {
	r, _ := os.Open(this.table + ".csv")
	defer r.Close()
	csvReader := csv.NewReader(r)

	result := make([][]string, 0)

	// записи из файла в формате .csv
	records, _ := csvReader.ReadAll()

	// прохожу по всем записям
	for _, v := range records {
		// затем проверяю, задано ли какое-то условие для фильтрации
		if this.where != nil {
			// если задано и оно проверка проходит удачно, то добавляю к результатам текущую запись
			if !this.where.Compute(v, this.schema) {
				result = append(result, v)
			}
		} else {
			break
		}
	}

	w, _ := os.Create(this.table + ".csv")
	defer w.Close()

	csvWriter := csv.NewWriter(w)
	csvWriter.WriteAll(result)
	csvWriter.Flush()
}
