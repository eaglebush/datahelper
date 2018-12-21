package main

import (
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/eaglebush/datatable"
)

// DataHelper struct
type DataHelper struct {
	db *sql.DB
	tx *sql.Tx
}

//NewDataHelper - creates a new DataHelper
func NewDataHelper() *DataHelper {
	dh := &DataHelper{}
	return dh
}

//Connect - connect to the database
func (dh *DataHelper) Connect(ConnectionString *string) (bool, error) {
	var err error

	dh.db, err = sql.Open("mssql", *ConnectionString)
	if err != nil {
		return false, err
	}

	err = dh.db.Ping()
	if err != nil {
		return false, err
	}
	return true, nil
}

//GetData - get data from the database
func (dh *DataHelper) GetData(preparedQuery string, arg ...interface{}) *datatable.DataTable {
	dt := datatable.NewDataTable("data")

	var rows *sql.Rows
	var err error
	if dh.tx != nil {
		rows, err = dh.tx.Query(preparedQuery, arg...)
	} else {
		rows, err = dh.db.Query(preparedQuery, arg...)
	}

	if err != nil {

	}
	defer rows.Close()

	colt, err := rows.ColumnTypes()

	if colt != nil && rows != nil {
		vals := make([]interface{}, len(colt))

		for i := 0; i < len(colt); i++ {
			vals[i] = new(interface{})
			length, _ := colt[i].Length()
			dt.AddColumn(colt[i].Name(), colt[i].ScanType(), length)
		}

		r := datatable.Row{}

		for rows.Next() {
			err = rows.Scan(vals...)
			if err != nil {
				continue
			}

			r = dt.NewRow()
			for i := 0; i < len(colt); i++ {
				v := vals[i].(*interface{})
				if *v != nil {
					r.Cells[i].Value = *v
				} else {
					t := colt[i].ScanType().Name()
					switch strings.ToLower(t) {
					case "bool":
						r.Cells[i].Value = false
					case "time":
						r.Cells[i].Value = time.Time{}
					case "string":
						r.Cells[i].Value = ""
					case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "unit16", "uint32", "unit64":
						r.Cells[i].Value = 0
					case "float32", "float64":
						r.Cells[i].Value = 0.0
					default:
						println("Unsupported type")
					}
				}
			}
			dt.AddRow(r)
		}
	}

	return dt
}

// Exec - execute queries that does not return rows such us INSERT, DELETE and UPDATE
func (dh *DataHelper) Exec(preparedQuery string, arg ...interface{}) (sql.Result, error) {
	var affected sql.Result
	var err error

	if dh.tx != nil {
		affected, err = dh.tx.Exec(preparedQuery, arg...)
	} else {
		affected, err = dh.db.Exec(preparedQuery, arg...)
	}

	if err != nil {
		log.Fatal(err)
	}
	return affected, err
}

// Begin - begins a new transaction
func (dh *DataHelper) Begin() (*sql.Tx, error) {
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
	return dh.tx.Commit()
}

// Rollback - rollbacks a transaction
func (dh *DataHelper) Rollback() error {
	if dh.tx == nil {
		return errors.New("No transaction was initiated")
	}
	return dh.tx.Rollback()
}

// Disconnect - disconnect from the database
func (dh *DataHelper) Disconnect() error {
	dh.tx = nil
	return dh.db.Close()
}
