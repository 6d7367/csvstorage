// Пример создания схемы с помощью функции CreateCSVTableSchema
// 	id := CSVField{"id", CSVFieldTypeInt}
// 	title := CSVField{"title", CSVFieldTypeText}
// 	CreateCSVTableSchema("table", id, title)
// будут созданы два файла table.csv и table.schema
//
// table.schema будет содержать:
// 	id,int
// 	title,text
//
package csvstorage

import (
	"encoding/csv"
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

type CSVField struct {
	FieldName, FieldType string
}

const (
	CSVFieldTypeInt  = "int"
	CSVFieldTypeText = "text"
)

func CreateCSVTableSchema(table string, fields ...CSVField) error {
	schema := make([][]string, 0)

	for _, f := range fields {
		record := make([]string, 2)
		record[0] = f.FieldName
		record[1] = f.FieldType

		schema = append(schema, record)
	}

	tableFilename := table + ".csv"
	schemaFilename := table + ".schema"
	w, err := os.Create(schemaFilename)
	defer w.Close()

	if err != nil {
		return err
	}

	w1, err := os.Create(tableFilename)
	defer w1.Close()

	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.WriteAll(schema)
	csvWriter.Flush()

	return nil
}
