package main

import (
	"os"
)

func main() {
	host := os.Getenv("HOST")
	database := os.Getenv("DATABASE")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")

	if len(host) == 0 || len(database) == 0 || len(user) == 0 || len(password) == 0 {
		panic("Missing configuration parameter")
	}

	db, err := NewMySQL(host, user, password, database)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	server := NewServer(db)
	err = server.Start(8080)
	if err != nil {
		panic(err.Error())
	}
}
