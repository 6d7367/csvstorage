package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

type CSVTableSchemaRecord struct {
	recordType string
	index      int
}

type CSVTableSchema map[string]CSVTableSchemaRecord

func ReadSchema(r io.Reader) CSVTableSchema {
	csvReader := csv.NewReader(r)
	schemaData, _ := csvReader.ReadAll()
	schema := make(CSVTableSchema)

	for k, v := range schemaData {
		r := CSVTableSchemaRecord{
			recordType: v[1],
			index:      k}

		schema[v[0]] = r
	}

	return schema
}

type SelectQuery struct {
	fields       []string
	table        string
	schema       CSVTableSchema
	fieldsToRead []int
}

type SelectQueryResultRecord map[int]string

type SelectQueryResult []SelectQueryResultRecord

func Select(fields ...string) *SelectQuery {
	q := new(SelectQuery)

	for _, v := range fields {
		q.fields = append(q.fields, v)
	}

	return q
}

func (this *SelectQuery) From(table string) *SelectQuery {
	r, _ := os.Open(table + ".schema")

	this.schema = ReadSchema(r)

	this.table = table

	for _, fieldName := range this.fields {
		this.fieldsToRead = append(this.fieldsToRead, this.schema[fieldName].index)
	}

	return this
}

func (this *SelectQuery) Do() SelectQueryResult {
	r, _ := os.Open(this.table + ".csv")

	csvReader := csv.NewReader(r)

	records, _ := csvReader.ReadAll()

	result := make(SelectQueryResult, 0)

	for _, v := range records {
		resultRecord := make(SelectQueryResultRecord)
		for _, fieldIndex := range this.fieldsToRead {

			resultRecord[fieldIndex] = v[fieldIndex]
		}
		result = append(result, resultRecord)
	}

	return result
}

func main() {
	fmt.Println(Select("content", "id").From("index").Do())
}
