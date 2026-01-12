package main

import (
	"log"

	"github.com/Devin-Yeung/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	defer srv.Close()

	err := srv.ListenAndServe()
	if err != nil {
		log.Fatal()
	}
}
