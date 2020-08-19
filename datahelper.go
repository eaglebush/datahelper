package datahelper

import (
	"database/sql"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	cfg "github.com/eaglebush/config"
	"github.com/eaglebush/datatable"
	//"eaglebush/datatable"
)

// DataHelper struct
type DataHelper struct {
	db                  *sql.DB
	tx                  *sql.Tx
	connectionString    string
	DriverName          string            // Driver name set in the configuration file
	ConnectionID        string            // Connection ID set in the configuration file
	AllQueryOK          bool              // Flags if all queries are ok in a non-transaction mode
	Errors              []string          // Errors encountered
	Settings            cfg.Configuration // Settings from the configuration
	CurrentDatabaseInfo *cfg.DatabaseInfo // Current database information
	RowLimitInfo        RowLimiting       // Row limiting information
}

// RowLimitPlacement - row limit placement of row limits
type RowLimitPlacement int

// Constants
const (
	RowLimitingFront RowLimitPlacement = 0 // The database query puts row limiting inside the SELECT clause
	RowLimitingRear  RowLimitPlacement = 1 // The database query puts row limiting at the end of the SELECT clause
)

// RowLimiting - row limiting setup
type RowLimiting struct {
	Keyword   string
	Placement RowLimitPlacement
}

// SingleRow struct
type SingleRow struct {
	HasResult bool
	Row       datatable.Row
}

// NewDataHelper - creates a new DataHelper
func NewDataHelper(config *cfg.Configuration) *DataHelper {

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

	return &DataHelper{
		Settings: *config,
	}
}

// Connect - connect to the database from configuration set in the NewDataHelper constructor.
func (dh *DataHelper) Connect(ConnectionID ...string) (bool, error) {
	var err error
	connID := ""

	if len(ConnectionID) > 0 {
		connID = ConnectionID[0]
	}

	dh.ConnectionID = connID
	if connID == "" {
		dh.ConnectionID = dh.Settings.DefaultDatabaseID
	}

	if dh.CurrentDatabaseInfo = dh.Settings.GetDatabaseInfo(dh.ConnectionID); dh.CurrentDatabaseInfo == nil {
		return false, errors.New("Connection Error: Connection ID does not exist")
	}

	di := dh.CurrentDatabaseInfo

	dh.DriverName = di.DriverName
	dh.RowLimitInfo = getRowLimiting(dh.DriverName)

	if len(di.ConnectionString) == 0 {
		return false, errors.New("Connection Error: Connection string is not set")
	}

	dh.connectionString = di.ConnectionString

	if dh.db, err = sql.Open(di.DriverName, di.ConnectionString); err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	if di.MaxOpenConnection != 0 {
		dh.db.SetMaxOpenConns(di.MaxOpenConnection)
	}

	if di.MaxIdleConnection != 0 {
		dh.db.SetMaxIdleConns(di.MaxIdleConnection)
	}

	if maxlt := di.MaxConnectionLifetime; maxlt != 0 {
		dh.db.SetConnMaxLifetime(time.Hour * time.Duration(maxlt))
	}

	if di.StorageType != "FILE" {
		if di.Ping {
			if err = dh.db.Ping(); err != nil {
				return false, errors.New("Connection Error: " + err.Error())
			}
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

// ConnectEx - connect via specified driver name and connection string
func (dh *DataHelper) ConnectEx(DriverName string, ConnectionString string, Ping bool) (bool, error) {
	var err error

	if len(DriverName) == 0 {
		return false, errors.New("Connection Error: DriverName is not set")
	}

	if len(ConnectionString) == 0 {
		return false, errors.New("Connection Error: Connection string is not set")
	}

	if dh.db, err = sql.Open(DriverName, ConnectionString); err != nil {
		return false, errors.New("Connection Error: " + err.Error())
	}

	dh.DriverName = DriverName
	dh.RowLimitInfo = getRowLimiting(dh.DriverName)

	di := dh.CurrentDatabaseInfo

	if di.MaxOpenConnection != 0 {
		dh.db.SetMaxOpenConns(di.MaxOpenConnection)
	}

	if di.MaxIdleConnection != 0 {
		dh.db.SetMaxIdleConns(di.MaxIdleConnection)
	}

	if maxlt := di.MaxConnectionLifetime; maxlt != 0 {
		dh.db.SetConnMaxLifetime(time.Hour * time.Duration(maxlt))
	}

	if Ping {
		if err = dh.db.Ping(); err != nil {
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
func (dh *DataHelper) GetRow(columns []string, tableNameWithParameters string, args ...interface{}) (SingleRow, error) {

	var (
		err   error
		row   *sql.Row
		cma   string
		query string
	)

	r := SingleRow{
		HasResult: false,
	}
	r.Row = datatable.Row{}

	if len(columns) == 0 {
		return r, errors.New("No column was specified")
	}

	if tableNameWithParameters == "" {
		return r, errors.New("No tablename was specified")
	}

	cma = ""
	query = "SELECT"
	for _, c := range columns {
		query += cma + " " + c
		cma = ","
	}
	query += " FROM "
	query += dh.replaceQueryParamMarker(tableNameWithParameters)

	if dh.tx != nil {
		row = dh.tx.QueryRow(query, args...)
	} else {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		row = dh.db.QueryRow(query, args...)
	}

	lencols := len(columns)
	r.Row.ResultRows = make([]interface{}, lencols)
	for i := 0; i < lencols; i++ {
		r.Row.ResultRows[i] = new(interface{})
	}

	norows := false

	if err = row.Scan(r.Row.ResultRows...); err != nil {

		norows = errors.Is(err, sql.ErrNoRows)
		if !norows {
			dh.Errors = append(dh.Errors, err.Error())
			dh.AllQueryOK = false
			return r, err
		}

		err = nil
	}

	r.Row.Cells = make([]datatable.Cell, lencols)
	r.Row.ColumnCount = lencols

	for i := 0; i < lencols; i++ {

		r.Row.Cells[i].ColumnName = getAliasFromColumnName(columns[i])
		r.Row.Cells[i].DBColumnType = ""
		r.Row.Cells[i].ColumnIndex = i
		r.Row.Cells[i].RowIndex = 0

		if !norows {
			v := r.Row.ResultRows[i].(*interface{})
			if *v != nil {
				r.Row.Cells[i].Value = *v
			} else {
				r.Row.Cells[i].Value = nil
			}
		}

	}

	r.HasResult = !norows

	return r, err
}

// GetData - get data from the database and return in a tabular form
func (dh *DataHelper) GetData(preparedQuery string, arg ...interface{}) (*datatable.DataTable, error) {

	dt := datatable.NewDataTable("data")

	var rows *sql.Rows
	var err error
	colsadded := false

	query := dh.replaceQueryParamMarker(preparedQuery)

	if dh.tx != nil {
		rows, err = dh.tx.Query(query, arg...)
	} else {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		rows, err = dh.db.Query(query, arg...)
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
	lencols := len(cols)
	vals := make([]interface{}, lencols)
	for i := 0; i < lencols; i++ {
		vals[i] = new(interface{})
	}

	r := datatable.Row{}

	for rows.Next() {

		if !colsadded {
			/* Column types for SQlite cannot be retrieved until .Next is called, so we need to retrieve it again */
			colt, _ := rows.ColumnTypes()
			for i := 0; i < len(colt); i++ {
				length, _ := colt[i].Length()
				dt.AddColumn(colt[i].Name(), colt[i].ScanType(), length, colt[i].DatabaseTypeName())
			}
			colsadded = true
		}

		if err = rows.Scan(vals...); err != nil {
			continue
		}

		r = dt.NewRow()
		for i := 0; i < lencols; i++ {
			v := vals[i].(*interface{})
			if *v != nil {
				r.Cells[i].Value = *v
			} else {
				r.Cells[i].Value = nil
			}
		}
		dt.AddRow(&r)
	}

	// Get possible error in the iteration
	err = rows.Err()

	return dt, err
}

// Exec - execute queries that does not return rows such us INSERT, DELETE and UPDATE
func (dh *DataHelper) Exec(preparedQuery string, arg ...interface{}) (sql.Result, error) {
	var result sql.Result
	var err error

	query := dh.replaceQueryParamMarker(preparedQuery)

	if dh.tx != nil {

		if result, err = dh.tx.Exec(query, arg...); err != nil {
			dh.AllQueryOK = false
			dh.Errors = append(dh.Errors, err.Error())
		}

		return result, err
	}

	if dh.tx == nil {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		return dh.db.Exec(query, arg...)
	}

	dh.AllQueryOK = false
	return nil, errors.New("Unknown execution error")
}

// Begin - begins a new transaction
func (dh *DataHelper) Begin() (*sql.Tx, error) {
	var tx *sql.Tx
	var err error

	dh.AllQueryOK = true

	if tx, err = dh.db.Begin(); err != nil {
		return nil, err
	}

	dh.tx = tx
	return tx, nil
}

// GetDataReader - returns a DataTable Row with an internal sql.Row object for iteration.
func (dh *DataHelper) GetDataReader(preparedQuery string, arg ...interface{}) (datatable.Row, error) {
	row := datatable.Row{}

	var rows *sql.Rows
	var err error

	query := dh.replaceQueryParamMarker(preparedQuery)

	if dh.tx != nil {
		rows, err = dh.tx.Query(query, arg...)
	} else {
		//If the query is not in a transaction, the following properties are always reset
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		rows, err = dh.db.Query(query, arg...)
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
	query := dh.replaceQueryParamMarker(preparedQuery)

	if dh.tx != nil {
		return dh.tx.Prepare(query)
	}

	if dh.db != nil {
		return dh.db.Prepare(query)
	}

	return nil, errors.New(`No active connections`)
}

// Disconnect - disconnect from the database
func (dh *DataHelper) Disconnect() error {
	defer func() { dh.db = nil }()

	dh.tx = nil

	if dh.db == nil {
		return nil
	}

	return dh.db.Close()
}

// IsInTransaction - checks whether the database is in transaction
func (dh *DataHelper) IsInTransaction() bool {
	return dh.tx != nil
}

// GetSequence - get the next sequence based on the sequence key
func (dh *DataHelper) GetSequence(SequenceKey string) (string, error) {
	var err error
	//var sr SingleRow
	var dt *datatable.DataTable

	if dh.ConnectionID == "" {
		dh.ConnectionID = dh.Settings.DefaultDatabaseID
	}

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
	if _, err = dh.Exec(upsertq); err != nil {
		return "", err
	}

	if dt, err = dh.GetData(resultq); err != nil {
		return "", err
	}

	if dt.RowCount > 0 {
		sq := dt.Rows[0].ValueInt64Ord(0)
		s := strconv.FormatInt(sq, 10)
		return s, nil
	}

	return "", nil
}

// Exists - checks if the record exists
func (dh *DataHelper) Exists(tableNameWithParameters string, args ...interface{}) (bool, error) {

	var (
		err     error
		row     *sql.Row
		query   string
		singval interface{}
	)

	if tableNameWithParameters == "" {
		return false, errors.New("No tablename was specified")
	}

	query = "SELECT "

	sel := ""
	rl := dh.RowLimitInfo
	if rl.Placement == RowLimitingFront {
		sel = rl.Keyword + " 1 1 AS Result"
	}

	if rl.Placement == RowLimitingRear {
		sel = "1 AS Result"
		tableNameWithParameters += " " + rl.Keyword + " 1"
	}

	query += sel + " FROM "
	query += dh.replaceQueryParamMarker(tableNameWithParameters)

	if dh.tx != nil {
		row = dh.tx.QueryRow(query, args...)
	} else {
		dh.AllQueryOK = true
		dh.Errors = make([]string, 0)

		row = dh.db.QueryRow(query, args...)
	}

	singval = new(interface{})

	if err = row.Scan(singval); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			dh.Errors = append(dh.Errors, err.Error())
			dh.AllQueryOK = false
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// ConnectionString - get the current connection string
func (dh *DataHelper) ConnectionString() string {
	return dh.connectionString
}

// SetMaxIdleConnection - max idle connection
func (dh *DataHelper) SetMaxIdleConnection(max int) {
	if dh.db == nil {
		return
	}
	dh.db.SetMaxIdleConns(max)
}

// SetMaxOpenConns - max open connection
func (dh *DataHelper) SetMaxOpenConns(max int) {
	if dh.db == nil {
		return
	}
	dh.db.SetMaxOpenConns(max)
}

// SetConnMaxLifetime - connection maximum lifetime
func (dh *DataHelper) SetConnMaxLifetime(d time.Duration) {
	if dh.db == nil {
		return
	}
	dh.db.SetConnMaxLifetime(d)
}

// Mark - starts a named transaction to simulate a save point
func (dh *DataHelper) Mark(PointID string) error {

	if dh.tx == nil {
		return errors.New("The current DataHelper instance is not in a built-in transaction")
	}

	if PointID == "" {
		return errors.New("No point id was specified")
	}

	// Get keyword from the config
	kw := `SAVE TRANSACTION`
	if km := dh.CurrentDatabaseInfo.KeywordMap; len(km) > 0 {
		for i := range km {
			if strings.ToLower(km[i].Key) == `savepoint_start` {
				kw = km[i].Value
				break
			}
		}
	}

	// Begin nested transaction
	if _, err := dh.Exec(kw + ` ` + PointID + `;`); err != nil {
		return err
	}

	return nil
}

// Discard - rejects a named transaction to simulate a save point
func (dh *DataHelper) Discard(PointID string) error {
	if dh.tx == nil {
		return errors.New("The current DataHelper instance is not in a built-in transaction")
	}

	if PointID == "" {
		return errors.New("No point id was specified to reject a save point")
	}

	// Get keyword from the config
	kw := `ROLLBACK TRANSACTION`
	if km := dh.CurrentDatabaseInfo.KeywordMap; len(km) > 0 {
		for i := range km {
			if strings.ToLower(km[i].Key) == `savepoint_release` {
				kw = km[i].Value
				break
			}
		}
	}

	// Begin nested transaction
	if _, err := dh.Exec(kw + ` ` + PointID + `;`); err != nil {
		return err
	}

	return nil
}

// Schemize adds the schema specified in the DatabaseInfo.Schema. If the setting is blank, it will not add any. If there is a dot in the table name, it will not add any.
func (dh *DataHelper) Schemize(tableName string) string {

	if dh.CurrentDatabaseInfo.Schema != "" {
		if posd := strings.Index(tableName, `.`); posd == -1 {

			// Get reserved word escape chars
			rwe := parseReserveWordsChars(dh.CurrentDatabaseInfo.ReservedWordEscapeChar)

			if strings.Index(tableName, rwe[0]) != -1 && strings.Index(tableName, rwe[1]) != -1 {
				return rwe[0] + dh.CurrentDatabaseInfo.Schema + rwe[1] + `.` + tableName
			}
			return dh.CurrentDatabaseInfo.Schema + `.` + tableName
		}
	}

	return tableName
}

func (dh *DataHelper) replaceQueryParamMarker(preparedQuery string) string {
	var paramchar string

	retstr := preparedQuery
	pattern := `\?` //search for ?

	paramseq := dh.CurrentDatabaseInfo.ParameterInSequence

	if paramchar = dh.CurrentDatabaseInfo.ParameterPlaceholder; paramchar == `?` {
		return retstr
	}

	re := regexp.MustCompile(pattern)
	matches := re.FindAllString(preparedQuery, -1)

	for i, match := range matches {
		if paramseq {
			retstr = strings.Replace(retstr, match, paramchar+strconv.Itoa((i+1)), 1)
		} else {
			retstr = strings.Replace(retstr, match, paramchar, 1)
		}
	}

	return retstr
}

func getRowLimiting(driverName string) RowLimiting {

	// default row limiting function (mysql, postgres and sqlite3)
	rl := RowLimiting{
		Keyword:   "LIMIT",
		Placement: RowLimitingRear,
	}

	switch driverName {
	case "mssql", "sqlserver":
		rl = RowLimiting{
			Keyword:   "TOP",
			Placement: RowLimitingFront,
		}
	}

	return rl
}

// get query column public name
func getAliasFromColumnName(queryColumnName string) string {
	res := queryColumnName

	// if the column name has 'AS'.
	if pos := strings.Index(strings.ToLower(res), ` as `); pos != -1 {
		res = strings.TrimSpace(queryColumnName[pos+3:])
	}

	// Check if it has brackets
	posl := strings.LastIndex(res, `[`)
	posr := strings.LastIndex(res, `]`)

	if posl != -1 && posr != -1 && posr > posl {
		pos := strings.LastIndex(res, ` `)

		// If the space is within the brackets, we will get the column name out from the inside
		if pos > posl && pos < posr {
			res = strings.TrimSpace(res[posl+1 : posr])
		}

		pos = strings.Index(res, `.`)
		if pos != -1 {
			res = strings.TrimSpace(res[posl+1 : posr])
		}
	}

	// parse spaced alias
	if posl == -1 || posr == -1 {

		pos := strings.LastIndex(res, ` `)
		if pos != -1 {
			res = strings.TrimSpace(res[pos:])
		}

		pos = strings.Index(res, `.`)
		if pos != -1 {
			res = strings.TrimSpace(res[pos+1:])
		}
	}

	return res
}

// parseReserveWordsChars always returns two-element array of opening and closing escape chars
func parseReserveWordsChars(ec string) []string {

	if len(ec) == 1 {
		return []string{ec, ec}
	}

	if len(ec) >= 2 {
		return []string{ec[0:1], ec[1:2]}
	}

	return []string{`"`, `"`} // default is double quotes
}
