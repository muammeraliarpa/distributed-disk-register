package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/muammeraliarpa/distributed-disk-register/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- YAPISAL TANIMLAR ---

type NodeMember struct {
	Address             string
	Client              pb.ReplicationServiceClient
	RegClient           pb.RegistrationServiceClient
	CurrentMessageCount int
}

type Node struct {
	pb.UnimplementedReplicationServiceServer
	pb.UnimplementedRegistrationServiceServer

	address         string
	storage         map[string]string
	diskPath        string
	mu              sync.Mutex
	isLeader        bool
	tolerance       int
	familyMembers   map[string]*NodeMember
	messageLocation map[string][]string
}

// --- KONFİGÜRASYON ---

func readTolerance() int {
	data, err := os.ReadFile("tolerance.conf")
	if err != nil {
		return 2
	}
	t, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return t
}


func (n *Node) RegisterMember(ctx context.Context, req *pb.RegistrationRequest) (*pb.RegistrationResponse, error) {
	addr := req.GetAddress()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.RegistrationResponse{Success: false}, nil
	}

	n.mu.Lock()
	n.familyMembers[addr] = &NodeMember{
		Address:   addr,
		Client:    pb.NewReplicationServiceClient(conn),
		RegClient: pb.NewRegistrationServiceClient(conn),
	}
	n.mu.Unlock()

	log.Printf("✅ [LİDER] %s katıldı. Liste senkronize ediliyor...", addr)
	n.syncAllMembers()

	return &pb.RegistrationResponse{Success: true}, nil
}


func (n *Node) syncAllMembers() {
	n.mu.Lock()
	var memberInfos []*pb.MembershipList_MemberInfo
	memberInfos = append(memberInfos, &pb.MembershipList_MemberInfo{
		Address: n.address,
		Count:   0,
	})
	for addr, m := range n.familyMembers {
		memberInfos = append(memberInfos, &pb.MembershipList_MemberInfo{
			Address: addr,
			Count:   int32(m.CurrentMessageCount),
		})
	}
	n.mu.Unlock()

	for addr, m := range n.familyMembers {
		go func(target string, member *NodeMember) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			defer cancel()
			member.RegClient.SyncMembership(ctx, &pb.MembershipList{Members: memberInfos})
		}(addr, m)
	}
}

func (n *Node) SyncMembership(ctx context.Context, req *pb.MembershipList) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for addr := range n.familyMembers {
		delete(n.familyMembers, addr)
	}

	for _, info := range req.Members {
		if info.Address == n.address {
			continue
		}
		conn, err := grpc.Dial(info.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			n.familyMembers[info.Address] = &NodeMember{
				Address:             info.Address,
				Client:              pb.NewReplicationServiceClient(conn),
				RegClient:           pb.NewRegistrationServiceClient(conn),
				CurrentMessageCount: int(info.Count),
			}
		}
	}
	return &pb.Empty{}, nil
}


func main() {
	fmt.Println("Node başlatılıyor...")
}
