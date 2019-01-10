package datahelper

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //SQl Server Driver
	"github.com/eaglebush/datatable"
	_ "github.com/mattn/go-sqlite3" //SQlite Driver
)

// DataHelper struct
type DataHelper struct {
	db         *sql.DB
	tx         *sql.Tx
	AllQueryOK bool
	Errors     []string
	DriverName string
}

//NewDataHelper - creates a new DataHelper
func NewDataHelper(driverName string) *DataHelper {

	return &DataHelper{DriverName: driverName}
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

//GetData - get data from the database
func (dh *DataHelper) GetData(preparedQuery string, arg ...interface{}) (*datatable.DataTable, error) {
	dt := datatable.NewDataTable("data")

	var rows *sql.Rows
	var err error
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
