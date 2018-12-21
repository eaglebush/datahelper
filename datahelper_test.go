/*
	datahelper and datatable test
*/

package datahelper

import (
	"fmt"
	"log"
	"testing"

	cfg "github.com/eaglebush/config"
	_ "github.com/eaglebush/datatable"
)

var config cfg.Configuration

func TestGetData(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")

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

			log.Printf("UserName: %s\r\n", r.Value("UserName").(string))
			log.Printf("Active: %v\r\n", r.Value("Active"))
			log.Printf("AppsHubAdmin: %v\r\n", r.Value("AppsHubAdmin"))
			log.Printf("DateLastLoggedIn: %v\r\n", r.Value("DateLastLoggedIn"))
			log.Printf("DisplayName: %s\r\n", r.Value("DisplayName"))
			log.Printf("EmailAddress: %s\r\n", r.Value("EmailAddress"))
			log.Printf("ForgotPasswordGUID: %s\r\n", r.Value("ForgotPasswordGUID"))
			log.Printf("GMT: %d\r\n", r.Value("GMT").(int64))
			log.Printf("GUID: %s\r\n", r.Value("GUID"))
			log.Printf("LDAPLogin: %v\r\n", r.Value("LDAPLogin"))
			log.Printf("MobileNo: %s\r\n", r.Value("MobileNo"))
			log.Printf("ProfileImageURL: %s\r\n", r.Value("ProfileImageURL"))
		}
	}
}

func TestExec(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")

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
	config, _ := cfg.LoadConfig("config.json")

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
