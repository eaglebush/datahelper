/*
	datahelper and datatable test
*/

package main

import (
	_ "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/eaglebush/datatable"
)

func TestGetData(t *testing.T) {
	Init()

	db := NewDataHelper()

	connected, _ := db.Connect(&config.ConnectionString)
	if connected {
		defer db.Disconnect()
		UserKey := 1
		dt := db.GetData(`SELECT
								UserName, Active, AppsHubAdmin, 
								DateLastLoggedIn, DisplayName, EmailAddress,
								ForgotPasswordGUID, GMT, GUID, LDAPLogin, MobileNo, ProfileImageURL
						   FROM tshUser WHERE UserKey = ?`, UserKey)
		if dt.RowCount > 0 {
			r := dt.Rows[0]

			fmt.Printf("UserName: %s\r\n", r.Value("UserName").(string))
			fmt.Printf("Active: %v\r\n", r.Value("Active"))
			fmt.Printf("AppsHubAdmin: %v\r\n", r.Value("AppsHubAdmin"))
			fmt.Printf("DateLastLoggedIn: %v\r\n", r.Value("DateLastLoggedIn"))
			fmt.Printf("DisplayName: %s\r\n", r.Value("DisplayName"))
			fmt.Printf("EmailAddress: %s\r\n", r.Value("EmailAddress"))
			fmt.Printf("ForgotPasswordGUID: %s\r\n", r.Value("ForgotPasswordGUID"))
			fmt.Printf("GMT: %d\r\n", r.Value("GMT").(int64))
			fmt.Printf("GUID: %s\r\n", r.Value("GUID"))
			fmt.Printf("LDAPLogin: %v\r\n", r.Value("LDAPLogin"))
			fmt.Printf("MobileNo: %s\r\n", r.Value("MobileNo"))
			fmt.Printf("ProfileImageURL: %s\r\n", r.Value("ProfileImageURL"))
		}
	}
}

func TestExec(t *testing.T) {
	Init()

	db := &DataHelper{}
	connected, _ := db.Connect(&config.ConnectionString)
	if connected {
		defer db.Disconnect()
		r, err := db.Exec(`UPDATE tshUser SET ProfileImageURL=? WHERE UserKey=?`, `https://www.yahoo.com`, 1)
		if err != nil {
			println("Error: " + err.Error())
		} else {
			affected, _ := r.RowsAffected()
			fmt.Printf("Result: %v", affected)
		}
	}
}

func TestTransactionExec(t *testing.T) {
	Init()

	db := &DataHelper{}
	connected, _ := db.Connect(&config.ConnectionString)
	if connected {
		defer db.Disconnect()
		db.Begin()

		r, err := db.Exec(`UPDATE tshUser SET ProfileImageURL=? WHERE UserKey=?`, `http://www.yahoo.com`, 1)
		if err != nil {
			db.Rollback()
			println("Error: " + err.Error())
		} else {
			affected, _ := r.RowsAffected()
			db.Commit()
			fmt.Printf("Result: %v", affected)
		}
	}
}

func Init() {
	file, err := os.Open("config/config.json")
	if err == nil {
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&config)
	}
}
