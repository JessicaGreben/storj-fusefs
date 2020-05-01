package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"storj.io/uplink"
)

func setupUplink(ctx context.Context, access, bucketname string) *uplink.Project {
	a, err := uplink.ParseAccess(access)
	if err != nil {
		log.Fatal("parseAccess ", err)
		return nil
	}
	project, err := uplink.OpenProject(ctx, a)
	if err != nil {
		log.Fatal("OpenProject ", err)
		return nil
	}

	// check that the bucket exists
	_, err = project.StatBucket(ctx, bucketname)
	if err != nil {
		log.Fatal("StatBucket ", err)
		return nil
	}
	return project
}

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
