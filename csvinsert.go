// Пример добавления новых записей в таблицу
//	r1 := make(csvstorage.CSVValueRecord)
// 	r1["id"] = "6"
// 	r1["title"] = "six"
// 	csvstorage.Insert("index").Values(r1).Do()
package csvstorage

import (
	"encoding/csv"
	"os"
)

type InsertQuery struct {
	table  string
	values []CSVValueRecord
	schema CSVTableSchema
}

type CSVValueRecord map[string]string

func Insert(table string) *InsertQuery {
	q := new(InsertQuery)
	r, _ := os.Open(table + ".schema")
	defer r.Close()

	q.schema = ReadSchema(r)

	q.table = table

	return q
}

func (this *InsertQuery) Values(values ...CSVValueRecord) *InsertQuery {

	for _, v := range values {
		this.values = append(this.values, v)
	}

	return this
}

func (this *InsertQuery) Do() {
	r, _ := os.Open(this.table + ".csv")
	defer r.Close()
	csvReader := csv.NewReader(r)

	// записи из файла в формате .csv
	records, _ := csvReader.ReadAll()

	fieldCount := len(this.schema)

	for _, v := range this.values {
		r := make([]string, fieldCount)

		for i := range r {
			r[i] = ""
		}

		for fieldName, fieldValue := range v {
			fieldIndex := this.schema[fieldName].index
			r[fieldIndex] = fieldValue
		}

		records = append(records, r)
	}

	w, _ := os.Create(this.table + ".csv")
	defer w.Close()

	csvWriter := csv.NewWriter(w)
	csvWriter.WriteAll(records)
	csvWriter.Flush()
}
