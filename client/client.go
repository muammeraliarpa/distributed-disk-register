package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

const LeaderAddress = "localhost:6666"

func main() {
	// 1. Lider sunucuya ( 5555 portunu alan düğümün açtığı 6666 portuna) bağlantı
	conn, err := net.Dial("tcp", LeaderAddress)
	if err != nil {
		fmt.Println("HATA: Lider sunucuya bağlanılamadı!")
		fmt.Printf("Not: Liderin (Port 5555) çalıştığından ve 6666 portunu açtığından emin olun.\n")
		log.Fatalf("Detay: %v", err)
	}
	defer conn.Close()

	log.Printf("Lider sunucuya (%s) başarıyla bağlandı.", LeaderAddress)
	fmt.Println("--------------------------------------------------")
	fmt.Println("Kullanılabilir Komutlar:")
	fmt.Println("  - SET <id> <mesaj>  (Örn: SET 101 Merhaba)")
	fmt.Println("  - GET <id>          (Örn: GET 101)")
	fmt.Println("  - exit              (Programdan çıkar)")
	fmt.Println("--------------------------------------------------")

	reader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print(" Komut > ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			break
		}
		if input == "" {
			continue
		}

		// 2 . Metin tabanlı komutu Lidere gönder (HaToKuse Protokolü)
		_, err = conn.Write([]byte(input + "\n"))
		if err != nil {
			log.Printf(" Gönderme hatası: %v", err)
			break
		}

		// 3. Liderden gelen yanıtı oku
		response, err := serverReader.ReadString('\n')
		if err != nil {
			log.Printf(" Yanıt okuma hatası: %v", err)
			break
		}

		// Yanıtı ekrana bas
		fmt.Printf(" Sunucu Yanıtı: %s", response)
	}

	log.Println("İstemci kapatılıyor.")
}
