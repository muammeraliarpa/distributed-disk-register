package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
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

func (n *Node) handleLeaderSet(messageID, content string) string {
	n.mu.Lock()
	var members []*NodeMember
	for _, m := range n.familyMembers {
		members = append(members, m)
	}
	n.mu.Unlock()

	if len(members) < n.tolerance {
		return "ERROR: Yetersiz üye"
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].CurrentMessageCount < members[j].CurrentMessageCount
	})

	targetMembers := members[:n.tolerance]
	actualSuccessCount := 0
	var successfullyWrittenMembers []*NodeMember

	for _, m := range targetMembers {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		resp, err := m.Client.ReplicateMessage(ctx, &pb.ReplicateRequest{
			MessageId:      messageID,
			MessageContent: content,
		})
		cancel()

		if err == nil && resp.Success {
			actualSuccessCount++
			successfullyWrittenMembers = append(successfullyWrittenMembers, m)
		}
	}

	if actualSuccessCount < n.tolerance {
		return "ERROR: Yazma başarısız, tolerans sağlanamadı"
	}

	n.mu.Lock()
	for _, m := range successfullyWrittenMembers {
		alreadyHasIt := false
		if owners, ok := n.messageLocation[messageID]; ok {
			for _, addr := range owners {
				if addr == m.Address {
					alreadyHasIt = true
					break
				}
			}
		}

		if !alreadyHasIt {
			m.CurrentMessageCount++
			n.messageLocation[messageID] = append(n.messageLocation[messageID], m.Address)
		}
	}
	n.mu.Unlock()

	n.syncAllMembers()
	return "OK"
}

func (n *Node) ReplicateMessage(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	id := req.GetMessageId()
	content := req.GetMessageContent()

	filePath := fmt.Sprintf("%s/%s.txt", n.diskPath, id)
	file, _ := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	writer := bufio.NewWriter(file)
	writer.WriteString(content)
	writer.Flush()
	file.Close()

	n.storage[id] = content

	return &pb.ReplicateResponse{Success: true}, nil
}


func startTCPServer(n *Node) {
	tcpLis, _ := net.Listen("tcp", ":6666")
	fmt.Println("🚀 Lider 6666 portunda istemci bekliyor...")
	for {
		conn, _ := tcpLis.Accept()
		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				cmd := strings.Fields(scanner.Text())
				if len(cmd) < 2 { continue }

				if cmd[0] == "SET" && len(cmd) >= 3 {
					res := n.handleLeaderSet(cmd[1], strings.Join(cmd[2:], " "))
					c.Write([]byte(res + "\n"))
				}
			}
		}(conn)
	}
}

func main() {
	basePort := 5555
	var lis net.Listener
	var currentPort int

	for p := basePort; p < 5600; p++ {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err == nil {
			lis = l
			currentPort = p
			break
		}
	}

	subDir := fmt.Sprintf("Data_%d", currentPort)
	fullPath := fmt.Sprintf("Data/%s", subDir)

	node := &Node{
		address:         fmt.Sprintf("localhost:%d", currentPort),
		storage:         make(map[string]string),
		diskPath:        fullPath,
		isLeader:        currentPort == basePort,
		familyMembers:   make(map[string]*NodeMember),
		messageLocation: make(map[string][]string),
		tolerance:       readTolerance(),
	}
	
	os.MkdirAll(node.diskPath, 0755)

	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, node)
	pb.RegisterRegistrationServiceServer(s, node)
	go s.Serve(lis)

	if node.isLeader {
		startTCPServer(node)
	} else {
		conn, err := grpc.Dial("localhost:5555", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := pb.NewRegistrationServiceClient(conn)
			client.RegisterMember(context.Background(), &pb.RegistrationRequest{Address: node.address})
		}
		select {}
	}
}
}
