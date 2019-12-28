package main

import "os"

type Configuration struct {
	DatabaseHost     string
	DatabaseName     string
	DatabaseUser     string
	DatabasePassword string
}

func ReadConfigurationFromEnv() Configuration {
	host := os.Getenv("HOST")
	database := os.Getenv("DATABASE")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")

	if len(host) == 0 || len(database) == 0 || len(user) == 0 || len(password) == 0 {
		panic("Missing configuration parameter")
	}

	var config Configuration
	config.DatabaseHost = host
	config.DatabaseName = database
	config.DatabaseUser = user
	config.DatabasePassword = password
	return config
}

type Data struct {
	Data string
}

type ListItem struct {
	Position int
	Data     Data
}

type Queue struct {
	Queue int
	Items []ListItem
}
