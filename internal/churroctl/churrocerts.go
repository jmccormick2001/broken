package churroctl

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"
)

const (
	servicecrt = "service.crt"
	servicekey = "service.key"
	clientcrt  = "client.%s.crt"
	clientkey  = "client.%s.key"
	nodecrt    = "node.crt"
	nodekey    = "node.key"
)

type ChurroCreds struct {
	Servicecrt    []byte
	Servicekey    []byte
	Clientrootcrt []byte
	Clientrootkey []byte
	Clientcrt     []byte
	Clientkey     []byte
	Nodecrt       []byte
	Nodekey       []byte
	Cacrt         []byte
	Cakey         []byte
}

func main() {

	rsaBits := 4096
	dur, _ := time.ParseDuration("300d")
	serviceHosts := "localhost,churro-watch,churro-loader,churro-ctl,127.0.0.1"
	pipelineName := "fuzzy"
	dbCreds, err := GenerateChurroCreds(pipelineName, serviceHosts, rsaBits, dur)
	if err != nil {
		panic(err)
	}

	dbCreds.Print(pipelineName)

}

func GenerateChurroCreds(pipelineName string, serviceHosts string, rsaBits int, validFor time.Duration) (dbCreds ChurroCreds, err error) {
	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Boerne"},
			StreetAddress: []string{"202 Lost Bridge"},
			PostalCode:    []string{"78006"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return dbCreds, err
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return dbCreds, err
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

	dbCreds.Cacrt = caPEM.Bytes()
	dbCreds.Cakey = caPrivKeyPEM.Bytes()
	//fmt.Printf("jeff ca.crt %s\n", string(caPEM.Bytes()))
	//fmt.Printf("jeff ca.key %s\n", string(caPrivKeyPEM.Bytes()))

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Boerne"},
			StreetAddress: []string{"202 Lost Bridge"},
			PostalCode:    []string{"78006"},
		},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames: []string{
			"node",
			"localhost",
			"cockroachdb-public",
			fmt.Sprintf("cockroachdb-public.%s", pipelineName),
			fmt.Sprintf("cockroachdb-public.%s.svc.client.local", pipelineName),
			"*.cockroachdb",
			fmt.Sprintf("*cockroachdb.%s", pipelineName),
			fmt.Sprintf("*.cockroachdb.%s.svc.cluster.local", pipelineName)},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return dbCreds, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return dbCreds, err
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	dbCreds.Nodecrt = certPEM.Bytes()
	dbCreds.Nodekey = certPrivKeyPEM.Bytes()
	//fmt.Printf("jeff %s %s\n", nodecrt, string(certPEM.Bytes()))
	//fmt.Printf("jeff %s %s\n", nodekey, string(certPrivKeyPEM.Bytes()))
	/**
	serverCert, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	if err != nil {
		return err
	}

	serverTLSConf = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(caPEM.Bytes())
	clientTLSConf = &tls.Config{
		RootCAs: certpool,
	}
	*/
	dbCreds.Clientrootcrt, dbCreds.Clientrootkey, err = genClientPair(ca, caPrivKey, "root")
	// by churro convention, we create a db client credential using
	// the same value as the pipeline name
	dbCreds.Clientcrt, dbCreds.Clientkey, err = genClientPair(ca, caPrivKey, pipelineName)

	dbCreds.Servicecrt, dbCreds.Servicekey, err = generateServiceCreds(serviceHosts, rsaBits, validFor)
	if err != nil {
		fmt.Printf("error %s\n", err.Error())
		os.Exit(1)
	}

	return dbCreds, nil
}

func genClientPair(ca *x509.Certificate, caPrivKey *rsa.PrivateKey, clientName string) (cBytes []byte, kBytes []byte, err error) {
	// set up our client certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			CommonName:    clientName,
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Boerne"},
			StreetAddress: []string{"202 Lost Bridge"},
			PostalCode:    []string{"78006"},
		},
		//		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:     []string{clientName, "DNS:root"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return cBytes, kBytes, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return cBytes, kBytes, err
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	//fmt.Printf("jeff client.%s.crt %s\n", clientName, string(certPEM.Bytes()))
	//fmt.Printf("jeff client.%s.key %s\n", clientName, string(certPrivKeyPEM.Bytes()))
	return certPEM.Bytes(), certPrivKeyPEM.Bytes(), nil
}

/**
host       = flag.String("host", "", "Comma-separated hostnames and IPs to generate a certificate for")
validFrom  = flag.String("start-date", "", "Creation date formatted as Jan 1 15:04:05 2011")
validFor   = flag.Duration("duration", 365*24*time.Hour, "Duration that certificate is valid for")
isCA       = flag.Bool("ca", false, "whether this cert should be its own Certificate Authority")
rsaBits    = flag.Int("rsa-bits", 2048, "Size of RSA key to generate. Ignored if --ecdsa-curve is set")
ecdsaCurve = flag.String("ecdsa-curve", "", "ECDSA curve to use to generate a key. Valid values are P224, P256 (recommended), P384, P521")
ed25519Key = flag.Bool("ed25519", false, "Generate an Ed25519 key")
*/

// --host=localhost,churro-watch,churro-loader,churro-ctl,127.0.0.1 --rsa-bits=4096
func generateServiceCreds(host string, rsaBits int, validFor time.Duration) (serviceCert []byte, serviceKey []byte, err error) {
	var isCA bool
	var ecdsaCurve string
	var ed25519Key bool
	var validFrom string

	if len(host) == 0 {
		return serviceCert, serviceKey, fmt.Errorf("host string is empty, requires comma separted list of host names")
	}

	var priv interface{}
	switch ecdsaCurve {
	case "":
		if ed25519Key {
			_, priv, err = ed25519.GenerateKey(rand.Reader)
		} else {
			priv, err = rsa.GenerateKey(rand.Reader, rsaBits)
			if err != nil {
				return serviceCert, serviceKey, err
			}
		}
	case "P224":
		priv, err = ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	case "P256":
		priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "P384":
		priv, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "P521":
		priv, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	default:
		return serviceCert, serviceKey, fmt.Errorf("Unrecognized elliptic curve: %q", ecdsaCurve)
	}
	if err != nil {
		return serviceCert, serviceKey, err
	}

	// ECDSA, ED25519 and RSA subject keys should have the DigitalSignature
	// KeyUsage bits set in the x509.Certificate template
	keyUsage := x509.KeyUsageDigitalSignature
	// Only RSA subject keys should have the KeyEncipherment KeyUsage bits set. In
	// the context of TLS this KeyUsage is particular to RSA key exchange and
	// authentication.
	if _, isRSA := priv.(*rsa.PrivateKey); isRSA {
		keyUsage |= x509.KeyUsageKeyEncipherment
	}

	var notBefore time.Time
	if len(validFrom) == 0 {
		notBefore = time.Now()
	} else {
		notBefore, err = time.Parse("Jan 2 15:04:05 2006", validFrom)
		if err != nil {
			return serviceCert, serviceKey, err
		}
	}

	notAfter := notBefore.Add(validFor)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return serviceCert, serviceKey, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	hosts := strings.Split(host, ",")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	if isCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		return serviceCert, serviceKey, err
	}

	serviceCert = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return serviceCert, serviceKey, err
	}
	serviceKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	//log.Printf("wrote %s\n", servicekey)

	return serviceCert, serviceKey, nil
}

func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	case ed25519.PrivateKey:
		return k.Public().(ed25519.PublicKey)
	default:
		return nil
	}
}

func (s *ChurroCreds) Print(pipelineName string) {
	fmt.Println("ChurroCreds...")
	fmt.Printf("service.crt %s\n", string(s.Servicecrt))
	fmt.Printf("service.key %s\n", string(s.Servicekey))
	fmt.Printf("client.root.crt %s\n", string(s.Clientrootcrt))
	fmt.Printf("client.root.key %s\n", string(s.Clientrootkey))
	fmt.Printf("client.%s.crt %s\n", pipelineName, string(s.Clientcrt))
	fmt.Printf("client.%s.key %s\n", pipelineName, string(s.Clientkey))
	fmt.Printf("node.crt %s\n", string(s.Nodecrt))
	fmt.Printf("node.key %s\n", string(s.Nodekey))
	fmt.Printf("ca.crt %s\n", string(s.Cacrt))
	fmt.Printf("ca.key %s\n", string(s.Cakey))
}
