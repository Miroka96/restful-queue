package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	createQueueTable = `
CREATE TABLE IF NOT EXISTS Queues (
    id int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name varchar(255) UNIQUE,
    INDEX queue_name_index (name)
                                  )`
	createItemTable = `
CREATE TABLE IF NOT EXISTS Items (
	queue int unsigned NOT NULL, 
	position int unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT, 
	data varchar(255) NOT NULL, 
	FOREIGN KEY (queue) REFERENCES Queues(id), 
	INDEX item_index (queue, position)
)
`
	createQueue            = "INSERT INTO Queues VALUES (NULL, NULL)"
	createNamedQueue       = "INSERT IGNORE INTO Queues VALUES (NULL, ?)"
	getNamedQueue          = "SELECT * FROM Queues WHERE name=?"
	appendItem             = "INSERT INTO Items (queue, data) VALUES (?, ?)"
	deleteItem             = "DELETE FROM Items WHERE position=?"
	getQueue               = "SELECT position, data FROM Items WHERE queue=? ORDER BY position"
	getQueueButExclude     = "SELECT position, data FROM Items WHERE queue=? AND position NOT IN (SELECT data FROM Items WHERE queue=?) ORDER BY position"
	getFirstItem           = getQueue + " ASC LIMIT 1"
	getFirstItemButExclude = getQueueButExclude + " ASC LIMIT 1"
	getRandomItem          = `
SELECT position, data 
FROM Items AS r1 
JOIN (
	SELECT CEIL(RAND() * (
		SELECT MAX(position) - MIN(position)
		FROM Items
	) + (
		SELECT MIN(position) 
		FROM Items
	)) AS id
) AS r2
WHERE r1.queue=? AND r1.position >= r2.id
ORDER BY r1.position ASC
LIMIT 1
`
	getRandomItemButExclude = `
SELECT position, data 
FROM Items AS r1 
JOIN (
	SELECT CEIL(RAND() * (
		SELECT MAX(position) - MIN(position)
		FROM Items
	) + (
		SELECT MIN(position) 
		FROM Items
	)) AS id
) AS r2
WHERE r1.queue=? AND r1.position >= r2.id AND r1.position NOT IN (SELECT data FROM Items WHERE queue=?)
ORDER BY r1.position ASC
LIMIT 1
`
	getLastItem           = getQueue + " DESC LIMIT 1"
	getLastItemButExclude = getQueueButExclude + " DESC LIMIT 1"
	getQueueSize          = "SELECT COUNT(*) FROM Items WHERE queue=?"
)

type MySQLStorage struct {
	db                      *sql.DB
	appendItem              *sql.Stmt
	deleteItem              *sql.Stmt
	getQueue                *sql.Stmt
	getFirstItem            *sql.Stmt
	getFirstItemButExclude  *sql.Stmt
	getRandomItem           *sql.Stmt
	getRandomItemButExclude *sql.Stmt
	getLastItem             *sql.Stmt
	getLastItemButExclude   *sql.Stmt
	getQueueSize            *sql.Stmt
	createNamedQueue        *sql.Stmt
	getNamedQueue           *sql.Stmt
}

func NewMySQL(config Configuration) (*MySQLStorage, error) {
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s)/%s",
			config.DatabaseUser,
			config.DatabasePassword,
			config.DatabaseHost,
			config.DatabaseName))
	if err != nil {
		return nil, err
	}
	storage := MySQLStorage{}
	storage.db = db

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

	getFirstItemButExcludeStatement, err := db.Prepare(getFirstItemButExclude)
	if err != nil {
		return nil, err
	}
	storage.getFirstItemButExclude = getFirstItemButExcludeStatement

	getRandomItemStatement, err := db.Prepare(getRandomItem)
	if err != nil {
		return nil, err
	}
	storage.getRandomItem = getRandomItemStatement

	getRandomItemButExcludeStatement, err := db.Prepare(getRandomItemButExclude)
	if err != nil {
		return nil, err
	}
	storage.getRandomItemButExclude = getRandomItemButExcludeStatement

	getLastItemStatement, err := db.Prepare(getLastItem)
	if err != nil {
		return nil, err
	}
	storage.getLastItem = getLastItemStatement

	getLastItemButExcludeStatement, err := db.Prepare(getLastItemButExclude)
	if err != nil {
		return nil, err
	}
	storage.getLastItemButExclude = getLastItemButExcludeStatement

	getQueueSizeStatement, err := db.Prepare(getQueueSize)
	if err != nil {
		return nil, err
	}
	storage.getQueueSize = getQueueSizeStatement

	createNamedQueueStatement, err := db.Prepare(createNamedQueue)
	if err != nil {
		return nil, err
	}
	storage.createNamedQueue = createNamedQueueStatement

	getNamedQueueStatement, err := db.Prepare(getNamedQueue)
	if err != nil {
		return nil, err
	}
	storage.getNamedQueue = getNamedQueueStatement

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

	err = storage.getQueue.Close()
	if err != nil {
		return err
	}

	err = storage.getFirstItem.Close()
	if err != nil {
		return err
	}

	err = storage.getFirstItemButExclude.Close()
	if err != nil {
		return err
	}

	err = storage.getRandomItem.Close()
	if err != nil {
		return err
	}

	err = storage.getRandomItemButExclude.Close()
	if err != nil {
		return err
	}

	err = storage.getLastItem.Close()
	if err != nil {
		return err
	}

	err = storage.getLastItemButExclude.Close()
	if err != nil {
		return err
	}

	err = storage.getQueueSize.Close()
	if err != nil {
		return err
	}

	err = storage.createNamedQueue.Close()
	if err != nil {
		return err
	}

	err = storage.getNamedQueue.Close()
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

// ignores already existing queues
func (storage *MySQLStorage) CreateNamedQueueIgnoring(name string) error {
	_, err := storage.createNamedQueue.Exec(name)
	return err
}

func (storage *MySQLStorage) GetCreateNamedQueue(name string) (*Queue, error) {
	err := storage.CreateNamedQueueIgnoring(name)
	if err != nil {
		return nil, err
	}

	results, err := storage.getNamedQueue.Query(name)
	if err != nil {
		return nil, err
	}

	results.Next()
	var id int
	var queueName string
	err = results.Scan(&id, &queueName)
	if err != nil {
		return nil, err
	}

	return storage.GetQueue(id)
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

func (storage *MySQLStorage) GetQueueSize(queue int) (int, error) {
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
			return size, err
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

func getElement(queue int, stmt *sql.Stmt) (*ListItem, error) {
	results, err := stmt.Query(queue)
	if err != nil {
		return nil, err
	}

	results.Next()
	var position int
	var dataString string
	err = results.Scan(&position, &dataString)
	if err != nil {
		return nil, err
	}

	var listItem = ListItem{
		Position: position,
		Data:     Data{dataString},
	}

	if results.Next() {
		// this condition is expected to be false
		log.Println("Got unexpected second result for first Item")
		// do not investigate any further
	}

	err = results.Close()
	if err != nil {
		return &listItem, err
	}

	return &listItem, nil
}

func getElementExcluding(elementQueue int, excludeQueue int, stmt *sql.Stmt) (*ListItem, error) {
	results, err := stmt.Query(elementQueue, excludeQueue)
	if err != nil {
		return nil, err
	}

	results.Next()
	var position int
	var dataString string
	err = results.Scan(&position, &dataString)
	if err != nil {
		return nil, err
	}

	var listItem = ListItem{
		Position: position,
		Data:     Data{dataString},
	}

	if results.Next() {
		// this condition is expected to be false
		log.Println("Got unexpected second result for first Item")
		// do not investigate any further
	}

	err = results.Close()
	if err != nil {
		return &listItem, err
	}

	return &listItem, nil
}

func (storage *MySQLStorage) pollElement(queue int, stmt *sql.Stmt) (*ListItem, error) {
	item, err := getElement(queue, stmt)
	if err != nil {
		return item, err
	}
	err = storage.Delete(item.Position)
	return item, err
}

func (storage *MySQLStorage) pollElementExcluding(elementQueue int, excludeQueue int, stmt *sql.Stmt) (*ListItem, error) {
	item, err := getElementExcluding(elementQueue, excludeQueue, stmt)
	if err != nil {
		return item, err
	}
	err = storage.Delete(item.Position)
	return item, err
}

func (storage *MySQLStorage) GetFirstElement(queue int) (*ListItem, error) {
	return getElement(queue, storage.getFirstItem)
}

func (storage *MySQLStorage) GetFirstElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return getElementExcluding(elementQueue, excludeQueue, storage.getFirstItem)
}

func (storage *MySQLStorage) PollFirstElement(queue int) (*ListItem, error) {
	return storage.pollElement(queue, storage.getFirstItem)
}

func (storage *MySQLStorage) PollFirstElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return storage.pollElementExcluding(elementQueue, excludeQueue, storage.getFirstItem)
}

func (storage *MySQLStorage) GetRandomElement(queue int) (*ListItem, error) {
	return getElement(queue, storage.getRandomItem)
}

func (storage *MySQLStorage) GetRandomElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return getElementExcluding(elementQueue, excludeQueue, storage.getRandomItemButExclude)
}

func (storage *MySQLStorage) PollRandomElement(queue int) (*ListItem, error) {
	return storage.pollElement(queue, storage.getRandomItem)
}

func (storage *MySQLStorage) PollRandomElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return storage.pollElementExcluding(elementQueue, excludeQueue, storage.getRandomItem)
}

func (storage *MySQLStorage) GetLastElement(queue int) (*ListItem, error) {
	return getElement(queue, storage.getLastItem)
}

func (storage *MySQLStorage) GetLastElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return getElementExcluding(elementQueue, excludeQueue, storage.getLastItem)
}

func (storage *MySQLStorage) PollLastElement(queue int) (*ListItem, error) {
	return storage.pollElement(queue, storage.getLastItem)
}

func (storage *MySQLStorage) PollLastElementExcluding(elementQueue int, excludeQueue int) (*ListItem, error) {
	return storage.pollElementExcluding(elementQueue, excludeQueue, storage.getLastItem)
}
