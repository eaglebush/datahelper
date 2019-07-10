/*
	PostGreSQl datahelper and datatable test
*/

package datahelper

import (
	"log"
	"testing"

	_ "github.com/denisenkom/go-mssqldb" //MSSQL Driver
	cfg "github.com/eaglebush/config"
)

func TestMSSQLGetData(t *testing.T) {
	config, _ := cfg.LoadConfig("config.mssql.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		dt, err := db.GetData(`SELECT GroupId, GroupName, GroupNameCode FROM tbcGroupName;`)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		for _, r := range dt.Rows {
			log.Printf("Code: %v\r\n", r.Value("GroupId"))
			log.Printf("Description: %v\r\n", r.Value("GroupName"))
			log.Printf("Value: %v\r\n", r.Value("GroupNameCode"))
			r.Close()
		}

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
			log.Printf("Error Begin Point: %v", err)
		}

		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag4','TestValue4',4,'U1', 'U2', 'U3');`)
		db.Exec(`INSERT INTO tshGenericLookUp (LookupTag, LookupValue, Ordinal, UserFld1, UserFld2, UserFld3) VALUES ('TestTag5','TestValue5',5,'U1', 'U2', 'U3');`)

		err = db.Discard("test")
		if err != nil {
			log.Printf("Error Reject Point: %v", err)
		}

		db.Exec("DELETE FROM tshGenericLookup")

		db.Commit()
	}
}
