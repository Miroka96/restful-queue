package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	createQueueTable = "CREATE TABLE IF NOT EXISTS Queues (id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT)"
	createItemTable  = "CREATE TABLE IF NOT EXISTS Items (" +
		"queue int unsigned NOT NULL, " +
		"position int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
		"data varchar(255) NOT NULL, " +
		"FOREIGN KEY (queue) REFERENCES Queues(id), " +
		"INDEX item_index (queue, position)" +
		")"
	createQueue  = "INSERT INTO Queues VALUES (NULL)"
	appendItem   = "INSERT INTO Items (queue, data) VALUES (?, ?)"
	deleteItem   = "DELETE FROM Items WHERE position=?"
	getQueue     = "SELECT position, data FROM Items WHERE queue=? ORDER BY position"
	getFirstItem = "SELECT position, data FROM Items WHERE queue=? ORDER BY position ASC LIMIT 1"
	getQueueSize = "SELECT COUNT(*) FROM Items WHERE queue=?"
)

type MySQLStorage struct {
	db           *sql.DB
	appendItem   *sql.Stmt
	deleteItem   *sql.Stmt
	getQueue     *sql.Stmt
	getFirstItem *sql.Stmt
	getQueueSize *sql.Stmt
}

func NewMySQL(host string, username string, password string, database string) (*MySQLStorage, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, database))
	if err != nil {
		return nil, err
	}
	storage := MySQLStorage{db,
		nil,
		nil,
		nil,
		nil,
		nil}

	err = storage.Initialize()
	if err != nil {
		return nil, err
	}

	appendStatement, err := db.Prepare(appendItem)
	if err != nil {
		return nil, err
	}
	storage.appendItem = appendStatement

	deleteStatement, err := db.Prepare(deleteItem)
	if err != nil {
		return nil, err
	}
	storage.deleteItem = deleteStatement

	getQueueStatement, err := db.Prepare(getQueue)
	if err != nil {
		return nil, err
	}
	storage.getQueue = getQueueStatement

	getFirstItemStatement, err := db.Prepare(getFirstItem)
	if err != nil {
		return nil, err
	}
	storage.getFirstItem = getFirstItemStatement

	getQueueSizeStatement, err := db.Prepare(getQueueSize)
	if err != nil {
		return nil, err
	}
	storage.getQueueSize = getQueueSizeStatement

	return &storage, nil
}

func (storage *MySQLStorage) Close() error {
	err := storage.appendItem.Close()
	if err != nil {
		return err
	}

	err = storage.deleteItem.Close()
	if err != nil {
		return err
	}

	err = storage.db.Close()
	if err != nil {
		return err
	}

	return nil
}

func (storage *MySQLStorage) Initialize() error {
	_, err := storage.db.Exec(createQueueTable)
	if err != nil {
		return err
	}
	_, err = storage.db.Exec(createItemTable)
	if err != nil {
		return err
	}

	return nil
}

func (storage *MySQLStorage) CreateQueue() (*Queue, error) {
	res, err := storage.db.Exec(createQueue)
	if err != nil {
		return nil, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &Queue{int(id), []ListItem{}}, nil
}

func (storage *MySQLStorage) Append(queue int, data Data) (*ListItem, error) {
	res, err := storage.appendItem.Exec(queue, data.Data)
	if err != nil {
		return nil, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &ListItem{int(id), data}, nil
}

func (storage *MySQLStorage) GetQueue(queue int) (*Queue, error) {
	results, err := storage.getQueue.Query(queue)
	if err != nil {
		return nil, err
	}

	var items = []ListItem{}
	for results.Next() {
		var position int
		var data Data
		err = results.Scan(&position, &data.Data)
		if err != nil {
			return nil, err
		}
		items = append(items, ListItem{position, data})
	}

	resultQueue := Queue{queue, items}

	err = results.Close()
	if err != nil {
		return &resultQueue, err
	}

	return &resultQueue, nil
}

func (storage *MySQLStorage) Delete(position int) error {
	_, err := storage.deleteItem.Exec(position)
	return err
}

func (storage *MySQLStorage) GetSize(queue int) (int, error) {
	results, err := storage.getQueueSize.Query(queue)
	if err != nil {
		return -1, err
	}

	results.Next()
	var size int
	err = results.Scan(&size)
	if err != nil {
		return -1, err
	}

	if results.Next() {
		// this condition is expected to be false
		log.Println("Got unexpected second result for queue size")
		var next int
		err = results.Scan(&next)
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Value = " + string(next))
		}
		// do not investigate any further
	}

	err = results.Close()
	if err != nil {
		return size, err
	}

	return size, nil
}

func (storage *MySQLStorage) GetFirstElement(queue int) (*ListItem, error) {
	//TODO
	return nil, nil
}

func (storage *MySQLStorage) GetRandomElement(queue int) (*ListItem, error) {
	//TODO
	return nil, nil
}
