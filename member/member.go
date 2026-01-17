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

// --- YARDIMCI FONKSİYONLAR ---

func readTolerance() int {
	data, err := os.ReadFile("tolerance.conf")
	if err != nil {
		return 2
	}
	t, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return t
}

// --- gRPC SERVİS METOTLARI ---

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

func (n *Node) ReplicateMessage(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	id := req.GetMessageId()
	content := req.GetMessageContent()
	filePath := fmt.Sprintf("%s/%s.txt", n.diskPath, id)

	fileBuf, _ := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	writer := bufio.NewWriter(fileBuf)
	writer.WriteString(content)
	writer.Flush()
	fileBuf.Close()

	n.storage[id] = content
	return &pb.ReplicateResponse{Success: true}, nil
}

func (n *Node) GetMessage(ctx context.Context, req *pb.GetMessageRequest) (*pb.GetMessageResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	content, exists := n.storage[req.GetMessageId()]
	if !exists {
		return &pb.GetMessageResponse{Found: false}, nil
	}
	return &pb.GetMessageResponse{Found: true, MessageContent: content}, nil
}

func (n *Node) GetStorageReport(ctx context.Context, req *pb.Empty) (*pb.StorageReportResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &pb.StorageReportResponse{MessageCount: int32(len(n.storage))}, nil
}

// --- RAPORLAMA FONKSİYONU ---

func (n *Node) startPeriodicReporting() {
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for range ticker.C {
			n.mu.Lock()
			if n.isLeader {
				fmt.Println("\n--- 👑 LİDER ANLIK DURUM RAPORU ---")
				for addr, m := range n.familyMembers {
					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					_, err := m.Client.GetStorageReport(ctx, &pb.Empty{})
					cancel()
					if err != nil {
						log.Printf("\n🔴 [SİSTEM] ÜYE ÖLDÜ: %s siliniyor!", addr)
						delete(n.familyMembers, addr)
						go n.syncAllMembers()
						continue
					}
				}
				fmt.Printf("Toplam Benzersiz Mesaj: %d\n", len(n.messageLocation))
				fmt.Println("Kümedeki Aktif Düğümler:")
				fmt.Printf("  ⭐ %-20s (BEN/LİDER)\n", n.address)
				for addr, m := range n.familyMembers {
					fmt.Printf("  📍 Üye: %-20s | 📁 Mesaj: %d\n", addr, m.CurrentMessageCount)
				}
			} else {
				fmt.Printf("\n--- 👤 ÜYE DURUM RAPORU [%s] ---\n", n.address)
				fmt.Printf("Yerel Depolama: %d mesaj\n", len(n.storage))
				fmt.Println("Küme Görünümü:")
				fmt.Println("  👑 localhost:5555 (LİDER)")
				fmt.Printf("  📍 %-20s (BEN) | 📁 Saklanan: %d\n", n.address, len(n.storage))
				for addr, m := range n.familyMembers {
					if addr == "localhost:5555" || addr == n.address { continue }
					fmt.Printf("  📍 %-20s       | 📁 Mesaj: %d\n", addr, m.CurrentMessageCount)
				}
			}
			n.mu.Unlock()
		}
	}()
}

// --- LİDER MANTIĞI VE TCP ---

func (n *Node) handleLeaderSet(messageID, content string) string {
	n.mu.Lock()
	
	existingOwners, exists := n.messageLocation[messageID]
	
	var targetMembers []*NodeMember
	
	var allMembers []*NodeMember
	for _, m := range n.familyMembers {
		allMembers = append(allMembers, m)
	}
	
	if exists && len(existingOwners) > 0 {
		for _, addr := range existingOwners {
			if m, ok := n.familyMembers[addr]; ok {
				targetMembers = append(targetMembers, m)
			}
		}
		if len(targetMembers) < n.tolerance {
			sort.Slice(allMembers, func(i, j int) bool {
				return allMembers[i].CurrentMessageCount < allMembers[j].CurrentMessageCount
			})
			
			for _, m := range allMembers {
				alreadyInList := false
				for _, t := range targetMembers {
					if t.Address == m.Address { alreadyInList = true; break }
				}
				
				if !alreadyInList {
					targetMembers = append(targetMembers, m)
					if len(targetMembers) == n.tolerance { break }
				}
			}
		}
	} else {
		sort.Slice(allMembers, func(i, j int) bool {
			return allMembers[i].CurrentMessageCount < allMembers[j].CurrentMessageCount
		})
		
		if len(allMembers) >= n.tolerance {
			targetMembers = allMembers[:n.tolerance]
		} else {
			targetMembers = allMembers
		}
	}
	n.mu.Unlock()

	if len(targetMembers) < n.tolerance {
		return "ERROR: Yetersiz üye"
	}

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
	var newOwnerList []string
	for _, m := range successfullyWrittenMembers {
		newOwnerList = append(newOwnerList, m.Address)
		
		isUpdate := false
		if exists {
			for _, oldAddr := range existingOwners {
				if oldAddr == m.Address { isUpdate = true; break }
			}
		}
		
		if !isUpdate {
			m.CurrentMessageCount++
		}
	}
	n.messageLocation[messageID] = newOwnerList
	n.mu.Unlock()

	n.syncAllMembers()
	return "OK"
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
	err := os.MkdirAll(node.diskPath, 0755)
	if err != nil {
		log.Fatalf("Klasör oluşturulamadı: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, node)
	pb.RegisterRegistrationServiceServer(s, node)
	go s.Serve(lis)

	node.startPeriodicReporting()

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
				} else if cmd[0] == "GET" {
					content, err := n.handleLeaderGet(cmd[1])
					if err != nil {
						c.Write([]byte("ERROR: " + err.Error() + "\n"))
					} else {
						c.Write([]byte("OK " + content + "\n"))
					}
				}
			}
		}(conn)
	}
}

// Liderin üyelerden veri çekme mantığı
func (n *Node) handleLeaderGet(id string) (string, error) {
	n.mu.Lock()
	locations := n.messageLocation[id]
	n.mu.Unlock()

	if len(locations) == 0 {
		return "", fmt.Errorf("mesaj sistemde bulunamadi")
	}

	// Mesajın olduğu bilinen üyeleri sırayla denediğ yer
	for _, addr := range locations {
		n.mu.Lock()
		m, exists := n.familyMembers[addr]
		n.mu.Unlock()
		
		if !exists { continue }

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := m.Client.GetMessage(ctx, &pb.GetMessageRequest{MessageId: id})
		cancel()

		if err == nil && resp.Found {
			return resp.MessageContent, nil
		}
	}
	return "", fmt.Errorf("mesaj kayitli üyelerden cekilemedi (üyeler kapali olabilir)")
}
