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
		var r bool
		switch op {
		case "=":
			r = value == test
		case "!=":
			r = value != test
		case "<":
			r = value < test
		case ">":
			r = value > test
		case "<=":
			r = value <= test
		case ">=":
			r = value >= test
		}

		return r
	}

	testStr := func(op, value, test string) bool {
		var r bool
		switch op {
		case "=":
			r = value == test
		case "!=":
			r = value != test
		case "<":
			r = value < test
		case ">":
			r = value > test
		case "<=":
			r = value <= test
		case ">=":
			r = value >= test
		}

		return r
	}

	fieldIndex := schema[this.field].index
	var r bool

	switch schema[this.field].recordType {
	case "int":
		fieldValue, _ := strconv.ParseInt(record[fieldIndex], 10, 32)
		testValue, _ := strconv.ParseInt(this.value, 10, 32)
		r = testInt(this.op, int(fieldValue), int(testValue))
	case "text":
		r = testStr(this.op, record[fieldIndex], this.value)
	}

	return r
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
	fields []string
	table  string
	where  WhereCondition
	limit  int
	schema CSVTableSchema
}

type SelectQueryResultRecord map[string]string

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

	outputFields := make(map[string]int)
	for _, f := range this.fields {
		outputFields[f] = this.schema[f].index
	}

	for _, v := range records {
		if len(result) >= this.limit && this.limit != 0 {
			break
		}

		selectQueryResultRecord := make(SelectQueryResultRecord)

		for fName, fIndex := range outputFields {
			selectQueryResultRecord[fName] = v[fIndex]
		}

		if this.where != nil {
			if this.where.Compute(v, this.schema) {
				result = append(result, selectQueryResultRecord)
			}
		} else {
			result = append(result, selectQueryResultRecord)
		}

	}

	return result
}

func main() {
	simpleWhere1 := NewSimpleWhere("id", "<", "3")
	simpleWhere2 := NewSimpleWhere("content", "=", "четыре")
	complexWhere := NewComplexWhereCondition("OR", simpleWhere1, simpleWhere2)
	q := Select("content", "id").From("index").Where(complexWhere)
	for _, r := range q.Do() {
		fmt.Println(r)
	}
}
