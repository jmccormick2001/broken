package config

import (
	"errors"
	"fmt"
	"os"

	"gitlab.com/churro-group/churro/api/v1alpha1"
)

type DBCredentials struct {
	SSLRootCertPath string
	SSLKeyPath      string
	SSLCertPath     string
}

func (d DBCredentials) Validate() error {
	_, err := os.Stat(d.SSLRootCertPath)

	if err != nil {
		return errors.New("--dbsslrootcert flag required")
	}

	_, err = os.Stat(d.SSLKeyPath)
	if err != nil {
		return errors.New("--dbsslkey flag required")
	}

	_, err = os.Stat(d.SSLCertPath)
	if err != nil {
		return errors.New("--dbsslcert flag required")
	}
	return nil

}

func (d DBCredentials) GetDBConnectString(src v1alpha1.Source) string {
	userid := src.Username
	hostname := src.Host
	port := src.Port
	database := src.Database
	sslrootcert := d.SSLRootCertPath
	sslkey := d.SSLKeyPath
	sslcert := d.SSLCertPath

	str := fmt.Sprintf("postgresql://%s@%s:%d/%s?ssl=true&sslmode=require&sslrootcert=%s&sslkey=%s&sslcert=%s", userid, hostname, port, database, sslrootcert, sslkey, sslcert)
	return str

}
