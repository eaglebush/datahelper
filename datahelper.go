package datahelper

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb" //SQl Server Driver
	cfg "github.com/eaglebush/config"
	"github.com/eaglebush/datatable"
	_ "github.com/mattn/go-sqlite3" //SQlite Driver
)

// DataHelper struct
type DataHelper struct {
	db         *sql.DB
	tx         *sql.Tx
	AllQueryOK bool
	Errors     []string
	Settings   cfg.Configuration
}

//SingleRow struct
type SingleRow struct {
	HasResult bool
	Row       datatable.Row
}

//NewDataHelper - creates a new DataHelper
func NewDataHelper(config *cfg.Configuration) *DataHelper {
	dh := &DataHelper{}
	dh.Settings = *config

	/*
		Reserve code for autosetting of struct fields

		c := reflect.ValueOf(config).Elem()
		t := reflect.ValueOf(dh).Elem()

		//Automatically load whatever is present in the configuration
		for i := 0; i < c.NumField(); i++ {
			f := c.Field(i)
			typeOfC := c.Type()
			cName := strings.ToLower(typeOfC.Field(i).Name)

			for d := 0; d < t.NumField(); d++ {
				g := t.Field(d)
				typeOfT := t.Type()
				dName := strings.ToLower(typeOfT.Field(d).Name)
				if cName == dName && t.CanSet() {
					g.SetString(f.Interface().(string))
					break
				}
			}
		}
	*/

	return dh
}

//Connect - connect to the database from configuration set in the NewDataHelper constructor
func (dh *DataHelper) Connect() (bool, error) {
	var err error

	if len(dh.Settings.ConnectionString) == 0 {
		return false, errors.New("Connection Error: Connection string is not set")
	}

	dh.db, err = sql.Open(dh.Settings.DriverName, dh.Settings.ConnectionString)
	if err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	if dh.Settings.DriverName != "sqlite3" {
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
	dt, err := dh.GetData(preparedQuery, args...)

	if err == nil {
		if dt.RowCount > 0 {
			r.HasResult = true
			r.Row.Cells = make([]datatable.Cell, dt.ColumnCount)
			r.Row.ColumnCount = dt.ColumnCount
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

	defer func() {
		if rows != nil {

			rows.Close()
		}
	}()

	if err != nil {
		dh.Errors = append(dh.Errors, err.Error())
		dh.AllQueryOK = false
		return dt, err
	}

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

	if dh.db == nil {
		return nil
	}

	return dh.db.Close()
}

//GetSequence - get the next sequence based on the sequence key
func (dh *DataHelper) GetSequence(SequenceKey string) (string, error) {
	si := &dh.Settings.SequenceInfo

	if len(si.UpsertQuery) == 0 {
		return "", errors.New("Sequence upsert query was not configured")
	}

	if len(si.ResultQuery) == 0 {
		return "", errors.New("Sequence result query was not configured")
	}

	if len(si.NamePlaceHolder) == 0 {
		return "", errors.New("Sequence name placeholder was not configured")
	}

	upsertq := strings.Replace(si.UpsertQuery, si.NamePlaceHolder, SequenceKey, -1)
	resultq := strings.Replace(si.ResultQuery, si.NamePlaceHolder, SequenceKey, -1)

	/* Update generator */
	_, err := dh.Exec(upsertq)
	if err != nil {
		return "", err
	}

	sr, err := dh.GetRow(resultq)
	if sr.HasResult {
		sq := sr.Row.ValueInt64(0)
		s := strconv.FormatInt(sq, 10)
		return s, err
	}

	return "", nil
}
