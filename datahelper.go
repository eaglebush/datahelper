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
	db               *sql.DB
	tx               *sql.Tx
	ConnectionID     string
	connectionString string
	DriverName       string
	AllQueryOK       bool
	Errors           []string
	Settings         cfg.Configuration
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

//Connect - connect to the database from configuration set in the NewDataHelper constructor.
//Put empty string in the ConnectionID parameter to get the default connection string
func (dh *DataHelper) Connect(ConnectionID string) (bool, error) {
	var err error

	dh.ConnectionID = ConnectionID
	if ConnectionID == "" {
		ConnectionID = dh.Settings.DefaultDatabaseID
	}

	conninfo := dh.Settings.GetDatabaseInfo(ConnectionID)
	if conninfo == nil {
		return false, errors.New("Connection Error: Connection ID does not exist")
	}

	dh.DriverName = conninfo.DriverName

	if len(conninfo.ConnectionString) == 0 {
		return false, errors.New("Connection Error: Connection string is not set")
	}

	dh.connectionString = conninfo.ConnectionString

	dh.db, err = sql.Open(conninfo.DriverName, conninfo.ConnectionString)
	if err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	if conninfo.StorageType != "FILE" {
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

//ConnectEx - connect via specified driver name and connection string
func (dh *DataHelper) ConnectEx(DriverName string, ConnectionString string, Ping bool) (bool, error) {
	var err error

	if len(DriverName) == 0 {
		return false, errors.New("Connection Error: DriverName is not set")
	}

	if len(ConnectionString) == 0 {
		return false, errors.New("Connection Error: Connection string is not set")
	}

	dh.db, err = sql.Open(DriverName, ConnectionString)
	if err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	dh.DriverName = DriverName

	if Ping {
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

	if err != nil {
		r.HasResult = false
		return r, err
	}

	if dt.RowCount == 0 {
		r.HasResult = false
		return r, nil
	}

	r.HasResult = true
	r.Row.Cells = make([]datatable.Cell, dt.ColumnCount)
	r.Row.ColumnCount = dt.ColumnCount
	for i := 0; i < len(r.Row.Cells); i++ {
		r.Row.Cells[i].ColumnIndex = i
		r.Row.Cells[i].ColumnName = dt.Columns[i].Name
		r.Row.Cells[i].RowIndex = 0
		r.Row.Cells[i].Value = dt.Rows[0].Cells[i].Value
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

	//log.Println("Driver: "+dh.DriverName, "ConnectionID: "+dh.ConnectionID)

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
			/* Column types for SQlite cannot be retrieved until .Next is called, so we need to retrieve it again */
			colt, _ := rows.ColumnTypes()
			for i := 0; i < len(colt); i++ {
				length, _ := colt[i].Length()
				dt.AddColumn(colt[i].Name(), colt[i].ScanType(), length, colt[i].DatabaseTypeName())
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
		dt.AddRow(&r)
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

//GetDataReader - returns a DataTable Row with an internal sql.Row object for iteration.
func (dh *DataHelper) GetDataReader(preparedQuery string, arg ...interface{}) (datatable.Row, error) {
	row := datatable.Row{}

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

	if err != nil {
		dh.Errors = append(dh.Errors, err.Error())
		dh.AllQueryOK = false
		return row, err
	}

	//Set the pointer to the returned rows
	row.SetSQLRow(rows)
	row.ResultRows = nil

	return row, err
}

// Commit - commits a transaction
func (dh *DataHelper) Commit() error {
	if dh.tx == nil {
		return errors.New("No transaction was initiated")
	}

	//The following properties are always reset after commit
	dh.AllQueryOK = true
	dh.Errors = make([]string, 0)
	defer func() {
		dh.tx = nil
	}()

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
	defer func() {
		dh.tx = nil
	}()
	return dh.tx.Rollback()
}

// Prepare - prepare a statement
func (dh *DataHelper) Prepare(preparedQuery string) (*sql.Stmt, error) {
	if dh.tx == nil {
		return nil, errors.New("Prepared statements need to have a transaction to function. No transaction was initiated")
	}

	return dh.tx.Prepare(preparedQuery)
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
	conninfo := dh.Settings.GetDatabaseInfo(dh.ConnectionID)

	si := &conninfo.SequenceGenerator

	if len(si.UpsertQuery) == 0 && len(si.ResultQuery) == 0 {
		return "", errors.New("Sequence upsert or result query was not configured")
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
	if err != nil {
		return "", err
	}

	if sr.HasResult {
		sq := sr.Row.ValueInt64(0)
		s := strconv.FormatInt(sq, 10)
		return s, nil
	}

	return "", nil
}

//ConnectionString - get the current connection string
func (dh *DataHelper) ConnectionString() string {
	return dh.connectionString
}
