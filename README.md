# Gelişmiş Dağıtık Disk Kayıt Sistemi (gRPC + TCP)

Bu proje; dinamik bir küme yapısında verilerin yüksek erişilebilirlik (High Availability) ve hata toleransı (Fault Tolerance) ilkelerine göre yönetildiği hibrit bir dağıtık sistem mimarisidir.

---

## 💻 Teknik Altyapı ve Neden Go?

Bu proje, yüksek performanslı ağ programlama yetenekleri ve eşzamanlılık (concurrency) desteği nedeniyle **Go (Golang)** dili ile geliştirilmiştir.

* **Concurrency (Goroutines & Channels):** Her bir istemci bağlantısı ve gRPC çağrısı, Go'nun hafif iş parçacıkları olan *goroutine*'ler ile yönetilir. Bu sayede sistem aynı anda yüzlerce isteği darboğaz yaşamadan işleyebilir.
* **Hız ve Hafiflik:** Go'nun derlenmiş bir dil olması, mikro-servis mimarilerinde çok düşük gecikme süreleri (latency) sunmasını sağlar.
* **Modern Network Stack:** Go'nun standart kütüphanesindeki `net` paketi ve `gRPC` kütüphanesi, TCP ve Protobuf haberleşmesini güvenli ve hızlı bir şekilde gerçekleştirmek için kullanılmıştır.

---

### 📦 Bağımlılıklar ve Versiyonlar

Projenin çalışması için gerekli ortam:

* **Dil:** Go 1.20+
* **Protokol:** gRPC / Protocol Buffers v3
* **İşletim Sistemi:** Linux, macOS, Windows

---





## 🛠 Temel Sistem Mekanizmaları

### 1. Dinamik Liderlik ve Rol Belirleme (Bootstrapping)

Sistemde "Lider" ve "Üye" ayrımı sabit bir konfigürasyona dayanmaz. Dinamiktir:

* **Lider Seçimi:** Sistem başlatıldığında her düğüm sırasıyla `5555` portunu dinlemeyi dener. Portu ilk rezerve eden düğüm otomatik olarak **Lider** rolünü üstlenir.
* **TCP Gateway:** Sadece Lider olan düğüm, dış dünyadan (Client) gelen istekleri kabul etmek için `6666` portu üzerinden bir **TCP Sunucusu** başlatır.
* **Üye Kaydı:** Diğer düğümler portları dolu bulduklarında sıradaki boş portu (`5556, 5557...`) alarak **Üye** olurlar ve Lidere gRPC üzerinden kayıt isteği (`RegisterMember`) gönderirler.

### 2. Ölüm Tespiti ve Kendi Kendini İyileştirme (Health Check & Failover)

Sistem, düğüm çökmelerine karşı "Self-Healing" özelliğine sahiptir:

* **Aktif İzleme:** Lider, her 3 saniyede bir kümedeki tüm üyelere düşük maliyetli gRPC "Ping" paketleri gönderir.
* **Hızlı Müdahale:** Eğer bir üye belirlenen zaman aşımı (300ms) içinde yanıt vermezse, Lider o üyeyi anında "Ölü" kabul eder ve aile listesinden siler.
* **Anlık Bildirim:** Lider, bir üyenin düştüğünü tespit eder etmez hayatta kalan diğer üyelere güncel listeyi fısıldar (**Broadcast**). Böylece tüm küme, ölen üyeden haberdar olur.

### 3. Akıllı Yük Dağılımı ve Replikasyon

Veri güvenliği ve performans için şu stratejiler uygulanır:

* **T-Hata Toleransı:** `tolerance.conf` dosyasındaki değer kadar kopya, sistemde saklanır. Örneğin 2 ise, her mesaj farklı 2 üyede yedeklenir.
* **En Az Yük İlkesi (Least Loaded):** Lider, yeni bir `SET` isteği aldığında üyeleri mesaj sayılarına göre sıralar ve veriyi en az meşgul olan üyelere yönlendirir.
* **Veri Lokasyonu Haritası:** Lider, hangi mesajın hangi düğümlerde olduğunu bir `Map` üzerinde tutar. `GET` isteği geldiğinde, veriyi kopyası olan üyelerden birinden dinamik olarak çeker.

### 4. Hiyerarşik ve Güvenli Depolama

* **Klasör Yapısı:** Veriler `Data/Data_[PORT]` yapısında saklanır. Bu sayede aynı fiziksel makinede çalışan düğümlerin disk alanları birbirine karışmaz.
* **Performans:** Yazma işlemlerinde Go'nun `bufio` kütüphanesi kullanılarak **Buffered I/O** performansı sağlanmıştır.

---

## 📁 Proje Yapısı

```text
hato-kuse/
├── proto/
│   ├── hato_kuse.proto           # gRPC servis ve mesaj tanımları
│   ├── hato_kuse.pb.go           # Mesaj yapıları (Structs)
│   └── hato_kuse_grpc.pb.go      # Servis arayüzleri (Client/Server interface)
├── member/
│   └── member.go                 # Hibrit Düğüm Mantığı (Leader/Follower)
├── client/
│   └── client.go                 # TCP/Text tabanlı test istemcisi
├── Data/                         # Verilerin port bazlı saklandığı ana dizin (Otomatik oluşturulur.)
├── tolerance.conf                # Replikasyon katsayısı
├── go.mod                        # Proje modül tanımı
└── go.sum                        # Bağımlılıkların checksum doğrulamaları
```

---

## 🚀 Çalıştırma ve Test Adımları

### 1. Projeyi Klonlayın ve Bağımlılıkları Yükleyin

```bash
git clone yazıcam
cd hato-kuse
go mod tidy
```

### 2. Kümenin Ayağa Kaldırılması

Birden fazla terminal açarak sistemi başlatın:

Bu projede client üzerinden TCP yoluyla lider ile iletişime geçmek için client dizininde bulunan client.go dosyasını ayrı bir terminalde çalıştırmak gerekmektedir.

```bash
#Lider 
go run member/member.go

#Üyeler (Bu komut istenilen üye sayısı kadar çalıştırılmalıdır.)
go run member/member.go

#Client
go run client/client.go

```

### 3. Hata Toleransı Testi

1. `client.go` üzerinden bir veri kaydedin: `SET 101 Merhaba`.
2. Liderin raporundan verinin hangi portlara (örn: 5556 ve 5557) yazıldığını görün.
3. Bu üyelerden birini (örn: 5556) `Ctrl+C` ile kapatın.
4. Liderin terminalinde `🔴 [SİSTEM] ÜYE ÖLDÜ` uyarısını bekleyin.
5. `GET 101` komutunu tekrar gönderin. Liderin veriyi hayatta kalan diğer üyeden (5557) çekip getirdiğini doğrulayın.

---

## 📊 Raporlama Formatı Örneği

Her 3 saniyede bir basılan rapor, kümenin anlık "röntgenini" çeker:

```text
--- 🖥️ DÜĞÜM RAPORU [localhost:5556] ---
Rol: 👤 ÜYE | Yerel Depolama: 2 mesaj
Kümedeki Tüm Aktif Düğümler:
  👑 localhost:5555 (LİDER)
  📍 localhost:5556 (BEN) | 📁 Saklanan: 2 mesaj
  📍 localhost:5557       | 📁 Mesaj: 2
----------------------------------

```


---

**Geliştiren:** Muammer Ali Arpa / 23060488

**Ders:** Sistem Programlama

