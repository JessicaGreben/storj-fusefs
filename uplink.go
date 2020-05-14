package main

import (
	"context"

	"storj.io/uplink"
)

func setupUplink(ctx context.Context, access, bucketname string) (*uplink.Project, error) {
	a, err := uplink.ParseAccess(access)
	if err != nil {
		return nil, err
	}
	project, err := uplink.OpenProject(ctx, a)
	if err != nil {
		return nil, err
	}

	// check that the bucket exists
	_, err = project.StatBucket(ctx, bucketname)
	if err != nil {
		return nil, err
	}
	return project, nil
}
