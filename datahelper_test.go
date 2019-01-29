/*
	datahelper and datatable test
*/

package datahelper

import (
	"fmt"
	"log"
	"testing"
	"time"

	cfg "github.com/eaglebush/config"
	_ "github.com/eaglebush/datatable"
)

var config cfg.Configuration

func TestGetData(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")

	db := NewDataHelper(config)

	connected, _ := db.Connect("")
	if connected {
		defer db.Disconnect()
		UserKey := 2
		dt, err := db.GetData(`SELECT
								UserName, Active, AppsHubAdmin, 
								DateLastLoggedIn, DisplayName, EmailAddress,
								ForgotPasswordGUID, GMT, GUID, LDAPLogin, 
								MobileNo, ProfileImageURL, ActivationCode, ActivationStatus
						   FROM USERACCOUNT WHERE UserKey = ?`, UserKey)
		if err == nil {
			if dt.RowCount > 0 {
				r := dt.Rows[0]

				log.Printf("UserName: %s\r\n", r.Value("UserName").(string))
				log.Printf("Active: %v\r\n", r.Value("Active"))
				log.Printf("AppsHubAdmin: %v\r\n", r.Value("AppsHubAdmin"))
				log.Printf("DateLastLoggedIn: %v\r\n", r.Value("DateLastLoggedIn"))
				log.Printf("DisplayName: %s\r\n", r.Value("DisplayName"))
				log.Printf("EmailAddress: %s\r\n", r.Value("EmailAddress"))
				log.Printf("ForgotPasswordGUID: %s\r\n", r.Value("ForgotPasswordGUID"))
				log.Printf("GMT: %f\r\n", r.Value("GMT").(float64))
				log.Printf("GUID: %s\r\n", r.Value("GUID"))
				log.Printf("LDAPLogin: %v\r\n", r.Value("LDAPLogin"))
				log.Printf("MobileNo: %s\r\n", r.Value("MobileNo"))
				log.Printf("ProfileImageURL: %s\r\n", r.Value("ProfileImageURL"))
				log.Printf("ActivationCode: %s\r\n", r.Value("ActivationCode"))
				log.Printf("ActivationStatus: %s\r\n", r.Value("ActivationStatus"))
			}
		}
	}
}

func TestGetDataSetValue(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")
	db := NewDataHelper(config)

	connected, _ := db.Connect("")
	if connected {
		defer db.Disconnect()
		UserKey := 2
		dt, err := db.GetData(`SELECT
								UserName, Active, AppsHubAdmin, 
								DateLastLoggedIn, DisplayName, EmailAddress,
								ForgotPasswordGUID, GMT, GUID, LDAPLogin, 
								MobileNo, ProfileImageURL, ActivationCode, ActivationStatus
						   FROM USERACCOUNT WHERE UserKey = ?`, UserKey)
		if err == nil {
			if dt.RowCount > 0 {
				r := dt.Rows[0]

				type UserAccount struct {
					UserKey            int       `json:"userkey,omitempty"`
					UserName           string    `json:"username,omitempty"`
					Password           string    `json:"password,omitempty"`
					Active             bool      `json:"active,omitempty"`
					AppsHubAdmin       bool      `json:"appshubadmin,omitempty"`
					DisplayName        string    `json:"displayname,omitempty"`
					ProfileImageURL    string    `json:"profileimageurl,omitempty"`
					DateLastLoggedIn   time.Time `json:"datelastloggedin,omitempty"`
					EmailAddress       string    `json:"emailaddress,omitempty"`
					MobileNo           string    `json:"mobileno,omitempty"`
					GMT                float64   `json:"gmt,omitempty"`
					GUID               string    `json:"guid,omitempty"`
					ForgotPasswordGUID string    `json:"forgotpasswordguid,omitempty"`
					LDAPLogin          bool      `json:"ldaplogin,omitempty"`
					ActivationCode     string    `json:"activationcode,omitempty"`
					ActivationStatus   string    `json:"activationstatus,omitempty"`
				}

				ua := UserAccount{}

				r.SetValue(&ua.UserName, "UserName")
				r.SetValue(&ua.Active, "Active")
				r.SetValue(&ua.AppsHubAdmin, "AppsHubAdmin")
				r.SetValue(&ua.DateLastLoggedIn, "DateLastLoggedIn")
				r.SetValue(&ua.DisplayName, "DisplayName")
				r.SetValue(&ua.EmailAddress, "EmailAddress")
				r.SetValue(&ua.ForgotPasswordGUID, "ForgotPasswordGUID")
				r.SetValue(&ua.GMT, "GMT")
				r.SetValue(&ua.GUID, "GUID")
				r.SetValue(&ua.LDAPLogin, "LDAPLogin")
				r.SetValue(&ua.MobileNo, "MobileNo")
				r.SetValue(&ua.ProfileImageURL, "ProfileImageURL")
				r.SetValue(&ua.ActivationCode, "ActivationCode")
				r.SetValue(&ua.ActivationStatus, "ActivationStatus")

				fmt.Println(&ua)
			}
		}
	}
}

func TestExec(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")
	db := NewDataHelper(config)
	connected, _ := db.Connect("")
	if connected {
		defer db.Disconnect()
		r, err := db.Exec(`UPDATE tshUser SET ProfileImageURL=? WHERE UserKey=?`, `http://www.yahoo.com`, 2)
		if err != nil {
			log.Printf("Error: " + err.Error())
		} else {
			affected, _ := r.RowsAffected()
			log.Printf("Result: %v", affected)
		}
	}
}

func TestTransactionExec(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")
	db := NewDataHelper(config)
	connected, _ := db.Connect("")
	if connected {
		defer db.Disconnect()
		db.Begin()

		r, err := db.Exec(`UPDATE tshUser SET ProfileImageURL=? WHERE UserKey=?`, `http://www.yahoo.com`, 2)
		if db.AllQueryOK {
			affected, _ := r.RowsAffected()
			db.Commit()
			log.Printf("Result: %v", affected)
		} else {
			db.Rollback()
			log.Printf("Error: " + err.Error())
		}
	}
}

func TestGetSequenceX(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")
	db := NewDataHelper(config)
	connected, err := db.Connect("")
	if err != nil {
		log.Printf("RErroresult: %v", err.Error())
	}
	if connected {
		defer db.Disconnect()
		db.Begin()

		key, err := db.GetSequence("USERACCOUNT")
		if db.AllQueryOK {
			db.Commit()
			log.Printf("Result: %v", key)
		} else {
			db.Rollback()
			log.Printf("Error: " + err.Error())
		}
	}
}
