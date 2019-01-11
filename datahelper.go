package datahelper

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"

	_ "github.com/denisenkom/go-mssqldb" //SQl Server Driver
	"github.com/eaglebush/datatable"
	_ "github.com/mattn/go-sqlite3" //SQlite Driver
)

// DataHelper struct
type DataHelper struct {
	db                      *sql.DB
	tx                      *sql.Tx
	AllQueryOK              bool
	Errors                  []string
	DriverName              string
	sequenceQuery           string
	sequenceNamePlaceHolder string
}

//SingleRow struct
type SingleRow struct {
	HasResult bool
	Row       datatable.Row
}

//NewDataHelper - creates a new DataHelper
func NewDataHelper(config interface{}) *DataHelper {

	t := reflect.ValueOf(config) //.Elem()
	//typeOfT := t.Type()
	//v := typeOfT.Field(0)
	v := reflect.Indirect(t).Field(0)
	z := v.Interface()
	print(z.(*interface{}))

	return &DataHelper{DriverName: ""}
}

//Connect - connect to the database
func (dh *DataHelper) Connect(ConnectionString *string) (bool, error) {
	var err error

	dh.db, err = sql.Open(dh.DriverName, *ConnectionString)
	if err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	if dh.DriverName != "sqlite3" {
		err = dh.db.Ping()
		if err != nil {
			return false, errors.New("Connection Error: " + err.Error())
		}
	}

	/*
		Resets errors and assumes all queries are OK.
		AllQueryOK is primarily used in a batch of queries.
		This will be set to false if one of the query execution fails.
	*/
	dh.Errors = make([]string, 0)
	dh.AllQueryOK = true

	return true, nil
}

// GetRow - get a single row result from a query
func (dh *DataHelper) GetRow(preparedQuery string, args ...interface{}) (SingleRow, error) {
	r := SingleRow{}
	dt, err := dh.GetData(preparedQuery, args)

	if err == nil {
		if dt.RowCount > 0 {
			r.HasResult = true
			r.Row.Cells = make([]datatable.Cell, dt.RowCount)
			for i := 0; i < len(r.Row.Cells); i++ {
				r.Row.Cells[i].ColumnIndex = i
				r.Row.Cells[i].ColumnName = dt.Columns[i].Name
				r.Row.Cells[i].RowIndex = 0
				r.Row.Cells[i].Value = dt.Rows[0].Cells[i].Value
			}
		}
	}

	return r, err
}

//GetData - get data from the database
func (dh *DataHelper) GetData(preparedQuery string, arg ...interface{}) (*datatable.DataTable, error) {
	dt := datatable.NewDataTable("data")

	var rows *sql.Rows
	var err error
	colsadded := false
	if dh.tx != nil {
		rows, err = dh.tx.Query(preparedQuery, arg...)
	} else {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		rows, err = dh.db.Query(preparedQuery, arg...)
	}

	defer rows.Close()

	if err == nil {
		if rows != nil {
			cols, _ := rows.Columns()
			vals := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				vals[i] = new(interface{})
			}

			r := datatable.Row{}

			for rows.Next() {
				if colsadded == false {
					colt, _ := rows.ColumnTypes()
					for i := 0; i < len(colt); i++ {
						length, _ := colt[i].Length()
						dt.AddColumn(colt[i].Name(), colt[i].ScanType(), length)
					}
					colsadded = true
				}

				err = rows.Scan(vals...)
				if err != nil {
					continue
				}

				r = dt.NewRow()
				for i := 0; i < len(cols); i++ {
					v := vals[i].(*interface{})
					if *v != nil {
						r.Cells[i].Value = *v
					} else {
						r.Cells[i].Value = nil
					}
				}
				dt.AddRow(r)
			}
		}
	} else {
		dh.Errors = append(dh.Errors, err.Error())
		dh.AllQueryOK = false
	}

	return dt, err
}

// Exec - execute queries that does not return rows such us INSERT, DELETE and UPDATE
func (dh *DataHelper) Exec(preparedQuery string, arg ...interface{}) (sql.Result, error) {
	var result sql.Result
	var err error

	if dh.tx != nil {
		result, err = dh.tx.Exec(preparedQuery, arg...)

		if err != nil {
			dh.AllQueryOK = false
			dh.Errors = append(dh.Errors, err.Error())
		}

		return result, err
	}

	if dh.tx == nil {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		return dh.db.Exec(preparedQuery, arg...)
	}

	dh.AllQueryOK = false
	return nil, errors.New("Unknown execution error")
}

// Begin - begins a new transaction
func (dh *DataHelper) Begin() (*sql.Tx, error) {
	dh.AllQueryOK = true

	tx, err := dh.db.Begin()

	if err != nil {
		return nil, err
	}
	dh.tx = tx
	return tx, nil
}

// Commit - commits a transaction
func (dh *DataHelper) Commit() error {
	if dh.tx == nil {
		return errors.New("No transaction was initiated")
	}

	//The following properties are always reset after commit
	dh.AllQueryOK = true
	dh.Errors = make([]string, 0)

	return dh.tx.Commit()
}

// Rollback - rollbacks a transaction
func (dh *DataHelper) Rollback() error {
	if dh.tx == nil {
		return errors.New("No transaction was initiated")
	}

	//The following properties are always reset after rollback
	dh.AllQueryOK = true
	dh.Errors = make([]string, 0)

	return dh.tx.Rollback()
}

// Disconnect - disconnect from the database
func (dh *DataHelper) Disconnect() error {
	dh.tx = nil
	return dh.db.Close()
}

//GetSequence - get the next sequence based on the sequence key
func (dh *DataHelper) GetSequence(SequenceKey string) (string, error) {
	if len(dh.sequenceQuery) == 0 {
		return "", errors.New("Sequence query was not yet setup")
	}

	q := strings.Replace(dh.sequenceQuery, dh.sequenceNamePlaceHolder, SequenceKey, -1)

	r, err := dh.GetRow(q)

	if err != nil {
		return "", err
	}

	if r.HasResult {
		return r.Row.ValueString(0), err
	}

	return "", nil
}
