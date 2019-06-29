/*
	PostGreSQl datahelper and datatable test
*/

package datahelper

import (
	"log"
	"testing"

	cfg "github.com/eaglebush/config"
	_ "github.com/eaglebush/datatable"
	_ "github.com/lib/pq" //PostGreSQL Driver
)

func TestPostgresGetData(t *testing.T) {
	config, _ := cfg.LoadConfig("config.json")

	db := NewDataHelper(config)

	connected, err := db.Connect()
	if err != nil {
		log.Printf("Error: %v", err)
	}

	if connected {
		defer db.Disconnect()
		dt, err := db.GetData(`SELECT code, description, value  FROM public.config;`)
		if err != nil {
			log.Printf("Error: %v", err)
		}
		for _, r := range dt.Rows {
			log.Printf("Code: %s\r\n", r.Value("code").(string))
			log.Printf("Description: %v\r\n", r.Value("description"))
			log.Printf("Value: %v\r\n", r.Value("value"))
			r.Close()
		}

	}
}
