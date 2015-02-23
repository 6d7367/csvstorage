// Пример использования составного where-условия:
// 	simpleWhere1 := csvstorage.NewSimpleWhere("id", "<", "3")
// 	simpleWhere2 := csvstorage.NewSimpleWhere("content", "=", "четыре")
// 	complexWhere := csvstorage.NewComplexWhereCondition("OR", simpleWhere1, simpleWhere2)
// 	q := csvstorage.Select("content", "id").From("index").Where(complexWhere)
package csvstorage

import "strconv"

// метод Compute должен выполнять фильтраци полей записи и указанных условий
type WhereCondition interface {
	Compute([]string, CSVTableSchema) bool
}

// простое условие, с равенством/неравенством
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

// сложное условие, может использовать логические операции AND, OR
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
