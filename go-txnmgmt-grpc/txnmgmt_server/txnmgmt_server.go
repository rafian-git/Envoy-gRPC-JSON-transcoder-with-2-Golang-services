package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	pb "example.com/txnmgmt/txnmgmt"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
)

const (
	port = ":50052"
)

func NewTxnManagementServer() *TxnManagementServer {
	return &TxnManagementServer{}
}

type TxnManagementServer struct {
	conn               *pgx.Conn
	first_txn_creation bool
}

func (server *TxnManagementServer) Run() error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTxnManagementServer(s, server)
	log.Printf("server listening at %v", lis.Addr())

	return s.Serve(lis)
}

func (server *TxnManagementServer) GetBalance(ctx context.Context, in *pb.UserId) (*pb.CurBlnc, error) {

	// select SUM(amount) AS Balance FROM txns where userid='user101' and opt='down';
	toAdd, err := server.conn.Query(context.Background(), "SELECT SUM(amount) AS Balance FROM txns WHERE userid = $1 AND opt = $2", in.GetUserId(), "up")
	if err != nil {
		return nil, err
	}
	
	addedBalance := pb.CurBlnc{}.Balance
	defer toAdd.Close()
	for toAdd.Next() {
		err = toAdd.Scan(&addedBalance)
		if err != nil {
			addedBalance = 0 
		}
	}
	log.Printf("Total Added Amount : %v", addedBalance) 

	toSubtract, err := server.conn.Query(context.Background(), "SELECT SUM(amount) AS Balance FROM txns WHERE userid = $1 AND opt = $2", in.GetUserId(), "down")
	if err != nil {
		return nil, err
	}
	withdrawnBalance := pb.CurBlnc{}.Balance
	defer toSubtract.Close()
	for toSubtract.Next() {
		err = toSubtract.Scan(&withdrawnBalance)
		if err != nil {
			withdrawnBalance = 0 
		}
	}
	log.Printf("Total Withdrawn Amount : %v", withdrawnBalance)
	returned_amount := &pb.CurBlnc{UserId: in.GetUserId(), Balance: addedBalance - withdrawnBalance}

	return returned_amount, nil

}

func (server *TxnManagementServer) TxnUp(ctx context.Context, in *pb.TxnInfo) (*pb.Amount, error) {

	createSql := `
	create table if not exists txns(
		id SERIAL PRIMARY KEY,
		userId text,
		amount int,
		opt text
	);
	`
	_, err := server.conn.Exec(context.Background(), createSql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Table creation failed: %v\n", err)
		os.Exit(1)
	}

	server.first_txn_creation = false

	log.Printf("Txn up for user # : %v , amount : %v", in.GetUserId(), in.GetAmount())

	created_txn := &pb.TxnInfo{UserId: in.GetUserId(), Amount: in.GetAmount()}
	returned_amount := &pb.Amount{Amount: in.GetAmount()}
	tx, err := server.conn.Begin(context.Background())
	if err != nil {
		log.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into txns(userId, amount, opt) values ($1,$2,$3)", created_txn.GetUserId(), created_txn.GetAmount(), "up")

	if err != nil {
		log.Fatalf("tx.Exec failed: %v", err)
	}
	tx.Commit(context.Background())
	return returned_amount, nil

}

func (server *TxnManagementServer) TxnDown(ctx context.Context, in *pb.TxnInfo) (*pb.Amount, error) {
	log.Printf("Txn check for user # : %v , amount : %v", in.GetUserId(), in.GetAmount())

	createSql := `
	create table if not exists txns(
		id SERIAL PRIMARY KEY,
		userId text,
		amount int,
		opt text
	);
	`
	_, err := server.conn.Exec(context.Background(), createSql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Table creation failed: %v\n", err)
		os.Exit(1)
	}

	server.first_txn_creation = false

	log.Printf("Txn up for user # : %v , amount : %v", in.GetUserId(), in.GetAmount())

	created_txn := &pb.TxnInfo{UserId: in.GetUserId(), Amount: in.GetAmount()}
	returned_amount := &pb.Amount{Amount: in.GetAmount()}
	tx, err := server.conn.Begin(context.Background())
	if err != nil {
		log.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into txns(userId, amount, opt) values ($1,$2,$3)", created_txn.GetUserId(), created_txn.GetAmount(), "down")

	if err != nil {
		log.Fatalf("tx.Exec failed: %v", err)
	}
	tx.Commit(context.Background())
	return returned_amount, nil

}

func main() {
	database_url := "postgres://reshop:737467@localhost:5432/txnmgmt"
	var txn_mgmt_server *TxnManagementServer = NewTxnManagementServer()
	conn, err := pgx.Connect(context.Background(), database_url)
	if err != nil {
		log.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close(context.Background())
	txn_mgmt_server.conn = conn
	txn_mgmt_server.first_txn_creation = true
	if err := txn_mgmt_server.Run(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}



//to generate pb.go file from proto file
//protoc -I txnmgmt/ txnmgmt/txnmgmt.proto --go_out=plugins=grpc:txnmgmt

//with http configs in proto file
//protoc -I txnmgmt/googleapi/ -I txnmgmt/ --include_imports --descriptor_set_out=txnmgmt/txnmgmt.pb txnmgmt/txnmgmt.proto
//protoc -I txnmgmt/googleapi/ -I txnmgmt/ txnmgmt/txnmgmt.proto --go_out=plugins=grpc:txnmgmt

//envoy --config-path txnmgmt/transcode_to_gRPC.yaml