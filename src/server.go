package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"strconv"
)

type Server struct {
	db     *MySQLStorage
	router *mux.Router
}

func NewServer(storage *MySQLStorage) *Server {
	router := mux.NewRouter()
	server := Server{storage, router}

	router.HandleFunc("/queues", server.createQueue).Methods("POST")
	router.HandleFunc("/queues/{id}", server.getQueue).Methods("GET")
	router.HandleFunc("/queues/{id}", server.appendItem).Methods("POST")
	router.HandleFunc("/items/{id}", server.deleteItem).Methods("DELETE")

	http.Handle("/", router)

	return &server
}

func (server *Server) Start(port int) error {
	return http.ListenAndServe(fmt.Sprintf(":%d", port), server.router)
}

func (server *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	queue, err := server.db.CreateQueue()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	response, err := json.Marshal(queue)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
}

func (server *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	queue, err := server.db.GetQueue(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	response, err := json.Marshal(queue)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
}

func (server *Server) appendItem(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	bodybytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body := string(bodybytes)
	item, err := server.db.Append(id, Data{Data: body})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}

	response, err := json.Marshal(item)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
}

func (server *Server) deleteItem(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = server.db.Delete(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		panic(err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
