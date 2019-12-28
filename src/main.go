package main

func main() {
	config := ReadConfigurationFromEnv()

	db, err := NewMySQL(config)
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
