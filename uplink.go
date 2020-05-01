package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"storj.io/uplink"
)

func read(ctx context.Context, p *uplink.Project, bucketname string) {
	object, err := p.DownloadObject(ctx, bucketname, "go.mod", nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	_, err = io.Copy(os.Stdout, object)
	if err != nil {
		log.Fatal("io.Copy: ", err)
	}
}

func ls(ctx context.Context, p *uplink.Project, bucketname string) {
	iter := p.ListObjects(ctx, bucketname, nil)
	for iter.Next() {
		fmt.Println(iter.Item().Key)
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj: ", err)
	}
}
