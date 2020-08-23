/*
	PostGreSQl datahelper and datatable test
*/

package datahelper

import (
	"database/sql"
	"log"
	"testing"

	_ "github.com/denisenkom/go-mssqldb" //MSSQL Driver
	cfg "github.com/eaglebush/config"
)

func TestParsePublicColumn(t *testing.T) {
	log.Println(getAliasFromColumnName(nil, `COUNT(*) AS CountX`))
	log.Println(getAliasFromColumnName(nil, `COUNT(*) CountX`))
	log.Println(getAliasFromColumnName(nil, `COUNT(*) Cou [ntX`))
	log.Println(getAliasFromColumnName(nil, `COUNT(*) AS [CountX You]`))
	log.Println(getAliasFromColumnName(nil, `COUNT(*) [CountX You]`))
	log.Println(getAliasFromColumnName(nil, `tr.WhatEver`))
	log.Println(getAliasFromColumnName(nil, `tr.WhatEver AS Whenever`))
	log.Println(getAliasFromColumnName(nil, `ISNULL(tr.WhatEver,'.') Howeverx`))
	log.Println(getAliasFromColumnName(nil, `ISNULL(tr.WhatEver,'.') AS Howevery`))
	log.Println(getAliasFromColumnName(nil, `tr.[Status]`))
}

func TestMSSQLGetData(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		dt, err := db.GetData(`SELECT user_name, display_name, ldap_login FROM useraccount WHERE user_key=@p1;`, 1)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		for _, r := range dt.Rows {
			log.Printf("Code: %v\r\n", r.Value("user_name"))
			log.Printf("Description: %v\r\n", r.Value("display_name"))
			//log.Printf("Value: %v\r\n", r.Value("appshub_admin"))
			log.Printf("AppsHubAdmin: %v\r\n", r.ValueBool("ldap_login"))
			r.Close()
		}

	}
}

func TestMSSQLGetDataNewConnected(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db, _, err := NewConnected(nil, config)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	defer db.Disconnect()

	dt, err := db.GetData(`SELECT user_name, display_name, ldap_login FROM useraccount WHERE user_key=@p1;`, 1)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	for _, r := range dt.Rows {
		log.Printf("Code: %v\r\n", r.Value("user_name"))
		log.Printf("Description: %v\r\n", r.Value("display_name"))
		//log.Printf("Value: %v\r\n", r.Value("appshub_admin"))
		log.Printf("AppsHubAdmin: %v\r\n", r.ValueBool("ldap_login"))
		r.Close()
	}

}

func TestGetRow(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect("DEFAULT")
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		//sr, err := db.GetRow([]string{`COUNT(*)`}, `applicationdomain WHERE application_key=? AND domain_key=?`, `3`, `1`)
		//sr, err := db.GetRow([]string{`COUNT(*) AS CountX`}, `applicationdomain WHERE application_key=? AND domain_key=?`, `3`, `1`)
		sr, err := db.GetRow([]string{`COUNT(*) AS [CountX You]`}, `applicationdomain WHERE application_key=? AND domain_key=?`, `3`, `1`)
		if err != nil {
			log.Printf("Error: %v", err)
			t.Fail()
		}
		if sr.HasResult {
			log.Printf("Data: %v", sr.Row.ValueInt64Ord(0))
			log.Printf("Data: %v", sr.Row.ValueInt64("CountX You"))
		}

	}
}

func TestExists(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect("LOCAL")
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()

		// db.RowLimitInfo.Keyword = "LIMIT"
		// db.RowLimitInfo.Placement = RowLimitingRear

		exists, err := db.Exists(`applicationdomain WHERE application_key=? AND domain_key=?`, `3`, `1`)
		if err != nil {
			log.Printf("Error: %v", err)
			t.Fail()
		}

		log.Printf("Exists: %v", exists)
	}
}

func TestSequence(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect("APPSLICDB")
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		key, err := db.GetSequence("Test")
		if err != nil {
			log.Printf("Error: %v", err)
		}
		log.Printf("Sequence: %s", key)
	}
}

func TestMSSQLGetRowWithWrongParameterTypes(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		sr, err := db.GetRow([]string{`COUNT(*)`}, `tcoTraderAddressClass WHERE TraderTypeID=? AND TraderAddrClassID=? AND TraderAddrClassKey<>?`, `CUSTOMER`, `CLASS1`, `1`)
		if err != nil {
			log.Printf("Error: %v", err)
			t.Fail()
		}
		if sr.HasResult {
			log.Printf("Data: %v", sr.Row.ValueInt64Ord(0))
		}

	}
}

func TestOutParameter(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()

		var NewNumber int
		NewNumber = 9
		_, err := db.Exec(`ssh_getnextnumber`, sql.Named(`SequenceName`, `TestOutParameter`), sql.Named(`NewNumber`, sql.Out{Dest: &NewNumber}))
		if err != nil {
			log.Printf("Error: %v", err)
		}
		log.Printf("Result: %v", NewNumber)
	}
}

func TestMSSQLNestedTransactions(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()

		db.Begin()

		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag1','TestValue1',1,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag2','TestValue2',2,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag3','TestValue3',3,'U1', 'U2', 'U3');`)

		err := db.Mark("test")
		if err != nil {
			log.Printf("Error Mark test: %v", err)
		}

		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag4','TestValue4',4,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag5','TestValue5',5,'U1', 'U2', 'U3');`)

		// err = db.Discard("test")
		// if err != nil {
		// 	log.Printf("Error Discard test: %v", err)
		// }

		err = db.Mark("test2")
		if err != nil {
			log.Printf("Error Mark test2: %v", err)
		}

		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag6','TestValue6',6,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag7','TestValue7',7,'U1', 'U2', 'U3');`)

		err = db.Discard("test2")
		if err != nil {
			log.Printf("Error Mark test3: %v", err)
		}

		err = db.Mark("test3")
		if err != nil {
			log.Printf("Error Save test3: %v", err)
		}

		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag8','TestValue8',8,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag9','TestValue9',9,'U1', 'U2', 'U3');`)

		// err = db.Discard("test3")
		// if err != nil {
		// 	log.Printf("Error Save test3: %v", err)
		// }

		//db.Exec("DELETE FROM tshGenericLookup")

		db.Commit()
	}
}

func TestReplaceParamChar(testing *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)
	_, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}
	defer db.Disconnect()
	db.CurrentDatabaseInfo.ParameterInSequence = true
	db.CurrentDatabaseInfo.ParameterPlaceholder = `@p`

	log.Println(db.replaceQueryParamMarker(`INSERT INTO Table (col1, col2, col3, ? AS Alias) VALUES (?,?,?);`))
}
