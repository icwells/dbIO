// Contains functions for mysqldump

package dbIO

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func (d *DBIO) getBackupFile(outdir string) string {
	// Returns output file name for mysqldump
	var name string
	datestamp := time.Now().Format("2006-01-02")
	if len(outdir) > 0 {
		name = filepath.Join(outdir, d.Database)
	} else {
		name = d.Database
	}
	return fmt.Sprintf("--result-file=%s.%s.sql", name, datestamp)
}

func (d *DBIO) getHost() string {
	// Returns formatted host name
	host := d.Host[:strings.Index(d.Host, ":")]
	host = host[strings.Index(host, "(")+1:]
	return fmt.Sprintf("-h%s", host)
}

// BackupDB calls mysldump to back up database to local machine
func (d *DBIO) BackupDB(outdir string) {
	d.logger.Printf("Backing up %s database to local machine...\n", d.Database)
	user := fmt.Sprintf("-u%s", d.User)
	host := d.getHost()
	password := fmt.Sprintf("-p%s", d.Password)
	outfile := d.getBackupFile(outdir)
	bu := exec.Command("mysqldump", user, host, password, outfile, d.Database, "--column-statistics=0")
	err := bu.Run()
	if err == nil {
		d.logger.Println("Backup complete.")
	} else {
		d.logger.Printf("Backup failed. %v\n", err)
	}
}
