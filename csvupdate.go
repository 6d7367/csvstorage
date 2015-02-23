// Пример обновление отдельных столбцов в таблице
// 	simpleWhere := csvstorage.NewSimpleWhere("id", "=", "5")
// 	r1 := make(csvstorage.CSVValueRecord)
// 	r1["title"] = "five"
// 	csvstorage.Update("index").Set(r1).Where(simpleWhere).Do()

package csvstorage

import (
	"encoding/csv"
	"os"
)

type UpdateQuery struct {
	table  string
	value  CSVValueRecord
	where  WhereCondition
	schema CSVTableSchema
}

func Update(table string) *UpdateQuery {
	q := new(UpdateQuery)

	r, _ := os.Open(table + ".schema")
	defer r.Close()

	q.schema = ReadSchema(r)

	q.table = table

	return q
}

func (this *UpdateQuery) Set(value CSVValueRecord) *UpdateQuery {
	this.value = value

	return this
}

func (this *UpdateQuery) Where(where WhereCondition) *UpdateQuery {
	this.where = where
	return this
}

func (this *UpdateQuery) Do() {
	r, _ := os.Open(this.table + ".csv")
	defer r.Close()
	csvReader := csv.NewReader(r)

	// записи из файла в формате .csv
	records, _ := csvReader.ReadAll()

	for k, r := range records {
		newRec := make([]string, len(r))

		for fN, fV := range r {
			newRec[fN] = fV
		}

		for fN, fV := range this.value {
			fI := this.schema[fN].index
			newRec[fI] = fV
		}

		if this.where != nil {
			if this.where.Compute(r, this.schema) {
				records[k] = newRec
			}
		} else {
			records[k] = newRec
		}
	}

	w, _ := os.Create(this.table + ".csv")
	defer w.Close()

	csvWriter := csv.NewWriter(w)
	csvWriter.WriteAll(records)
	csvWriter.Flush()
}
