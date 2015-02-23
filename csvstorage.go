package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
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

type WhereCondition interface {
	Compute([]string, CSVTableSchema) bool
}

type WhereSimpleCondition struct {
	field, op, value string
}

func NewSimpleWhere(field, op, value string) WhereSimpleCondition {
	c := WhereSimpleCondition{
		field: field,
		op:    op,
		value: value,
	}

	return c
}

func (this WhereSimpleCondition) Compute(record []string, schema CSVTableSchema) bool {

	testInt := func(op string, value, test int) bool {
		switch op {
		case "=":
			return value == test
		case "!=":
			return value != test
		case "<":
			return value < test
		case ">":
			return value > test
		case "<=":
			return value <= test
		case ">=":
			return value >= test
		}

		return false
	}

	testStr := func(op, value, test string) bool {
		switch op {
		case "=":
			return value == test
		case "!=":
			return value != test
		case "<":
			return value < test
		case ">":
			return value > test
		case "<=":
			return value <= test
		case ">=":
			return value >= test
		}

		return false
	}

	fieldIndex := schema[this.field].index

	switch schema[this.field].recordType {
	case "int":
		fieldValue, _ := strconv.ParseInt(record[fieldIndex], 10, 32)
		testValue, _ := strconv.ParseInt(this.value, 10, 32)
		return testInt(this.op, int(fieldValue), int(testValue))
	case "string":
		return testStr(this.op, record[fieldIndex], this.value)
	}

	return false
}

type WhereComplexCondition struct {
	logicCond    string
	cond1, cond2 WhereCondition
}

func NewComplexWhereCondition(logicCond string, cond1, cond2 WhereCondition) WhereComplexCondition {
	c := WhereComplexCondition{
		logicCond: logicCond,
		cond1:     cond1,
		cond2:     cond2,
	}

	return c
}

func (this WhereComplexCondition) Compute(record []string, schema CSVTableSchema) bool {
	r1 := this.cond1.Compute(record, schema)
	r2 := this.cond2.Compute(record, schema)

	var r bool

	switch this.logicCond {
	case "AND":
		r = r1 && r2
	case "OR":
		r = r1 || r2
	}

	return r
}

type SelectQuery struct {
	fields       []string
	table        string
	where        WhereCondition
	limit        int
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

func (this *SelectQuery) Where(cond WhereCondition) *SelectQuery {
	this.where = cond

	return this
}

func (this *SelectQuery) Limit(limit int) *SelectQuery {
	this.limit = limit

	return this
}

func (this *SelectQuery) Do() SelectQueryResult {
	r, _ := os.Open(this.table + ".csv")

	csvReader := csv.NewReader(r)

	records, _ := csvReader.ReadAll()

	result := make(SelectQueryResult, 0)

	for _, v := range records {
		if len(result) >= this.limit && this.limit != 0 {
			break
		}

		if this.where.Compute(v, this.schema) {
			resultRecord := make(SelectQueryResultRecord)
			for _, fieldIndex := range this.fieldsToRead {

				resultRecord[fieldIndex] = v[fieldIndex]
			}
			result = append(result, resultRecord)
		}

	}

	return result
}

func main() {
	simpleWhere1 := NewSimpleWhere("id", ">", "1")
	simpleWhere2 := NewSimpleWhere("id", "=", "4")
	complexWhere := NewComplexWhereCondition("AND", simpleWhere1, simpleWhere2)
	q := Select("content", "id").From("index").Limit(3).Where(complexWhere)
	for _, r := range q.Do() {
		for _, f := range r {
			fmt.Print(f, " ")
		}
		fmt.Println()
	}
}
