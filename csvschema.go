package csvstorage

import (
	"encoding/csv"
	"io"
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
