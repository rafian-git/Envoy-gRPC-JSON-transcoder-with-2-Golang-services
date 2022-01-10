package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"time"

	pb "example.com/usermgmt/usermgmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

func NewUserManagementServer() *UserManagementServer {
	return &UserManagementServer{}
}

type UserManagementServer struct {
	conn                *pgx.Conn
	first_user_creation bool
}

func (server *UserManagementServer) Run() error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterUserManagementServer(s, server)
	log.Printf("server listening at %v", lis.Addr())

	return s.Serve(lis)
}

func (server *UserManagementServer) CreateNewUser(ctx context.Context, in *pb.NewUser) (*pb.User, error) {

	createSql := `
	create table if not exists users(
		id SERIAL PRIMARY KEY,
		username text NOT NULL UNIQUE,
		password text,
		age int
	);
	`
	_, err := server.conn.Exec(context.Background(), createSql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Table creation failed: %v\n", err)
		os.Exit(1)
	}

	server.first_user_creation = false

	log.Printf("Received: %v", in.GetUsername())

	created_user := &pb.User{Username: in.GetUsername(), Password: in.GetPassword(), Age: in.GetAge()}
	tx, err := server.conn.Begin(context.Background())
	if err != nil {
		log.Fatalf("conn.Begin failed: %v", err)
	}

	_, err = tx.Exec(context.Background(), "insert into users(username, password, age) values ($1,$2,$3)",
		created_user.Username, created_user.Password, created_user.Age)
	if err != nil {
		log.Fatalf("tx.Exec failed: %v", err)
	}
	tx.Commit(context.Background())
	return created_user, nil
}

func (server *UserManagementServer) UserLogin(ctx context.Context, in *pb.LoginReq) (*pb.LoginRes, error) {

	rows, err := server.conn.Query(context.Background(), "select * from users where username=$1 and password=$2;", in.GetUsername(), in.GetPassword())
	if err != nil {
		return nil, err
	}
	loginData := &pb.User{}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&loginData.Id, &loginData.Username, &loginData.Password, &loginData.Age)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
	}
	if in.GetPassword() != loginData.Password || in.GetUsername() != loginData.Username {
		log.Fatalf("Invalid username or password")
	}
	loginResponse := &pb.LoginRes{Token: CreateToken(loginData.Username, loginData.Password)}
	return loginResponse, nil
}

func CreateToken(username string, password string) (tokenString string) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss":      "user-management-server",
		"aud":      "user-management-server",
		"nbf":      time.Now().Unix(),
		"exp":      time.Now().Add(time.Hour).Unix(),
		"sub":      "user",
		"username": username,
		"password": password,
	})
	tokenString, err := token.SignedString([]byte("verysecret"))
	if err != nil {
		panic(err)
	}
	return tokenString
}

func main() {
	database_url := "postgres://reshop:737467@localhost:5432/usermgmt"
	var user_mgmt_server *UserManagementServer = NewUserManagementServer()
	conn, err := pgx.Connect(context.Background(), database_url)
	if err != nil {
		log.Fatalf("Unable to establish connection: %v", err)
	}
	defer conn.Close(context.Background())
	user_mgmt_server.conn = conn
	user_mgmt_server.first_user_creation = true
	if err := user_mgmt_server.Run(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
