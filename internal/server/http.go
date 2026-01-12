package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func NewHTTPServer(addr string) *http.Server {
	s := newHTTPServer()
	r := mux.NewRouter()

	r.HandleFunc("/", s.handleProduce).Methods("POST")
	r.HandleFunc("/", s.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	resp := ProduceResponse{
		Offset: offset,
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	record, err := s.Log.Read(req.Offset)
	if errors.Is(err, ErrOffsetNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := ConsumeResponse{
		Record: record,
	}

	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ProduceRequest represents a request to produce a log record.
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse represents a response after producing a log record.
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest represents a request to consume a log record.
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse represents a response after consuming a log record.
type ConsumeResponse struct {
	Record Record `json:"record"`
}
