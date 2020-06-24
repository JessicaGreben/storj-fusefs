package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"storj.io/uplink"
)

var (
	uid = uint32(os.Geteuid())
	gid = uint32(os.Getegid())
)

func logE(msg string, err error) error {
	if err != nil {
		log.Printf("%s error: %+v\n", msg, err)
	}
	return err
}

type FS struct {
	root fs.Node
}

var _ fs.FS = (*FS)(nil)

func NewFS(project *uplink.Project, bucketname string, prefix string) *FS {
	return &FS{
		root: NewDir(project, bucketname, prefix),
	}
}

func (fs FS) Root() (fs.Node, error) {
	return fs.root, nil
}

type Dir struct {
	bucketname string
	prefix     string
	project    *uplink.Project
}

var _ fs.Node = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)

func NewDir(project *uplink.Project, bucketname, prefix string) *Dir {
	return &Dir{
		bucketname: bucketname,
		prefix:     prefix,
		project:    project,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Println("dir.Attr called")
	a.Mode = os.ModeDir | 0o550
	a.Uid = uid
	a.Gid = gid
	return nil
}

func (d *Dir) Lookup(ctx context.Context, objectKey string) (fs.Node, error) {
	log.Println("dir.Lookup called")

	objectKey = d.prefix + objectKey
	downloader, err := d.project.DownloadObject(context.Background(),
		d.bucketname,
		objectKey,
		&uplink.DownloadOptions{Length: int64(-1)},
	)
	if err != nil {
		if errors.Is(err, uplink.ErrObjectNotFound) {
			if ok := isDir(ctx, d, objectKey); ok {
				return NewDir(d.project, d.bucketname, objectKey+"/"), nil
			}
			return nil, logE("lookup object or dir does not exist", syscall.ENOENT)
		}
		return nil, logE("DownloadObject", err)
	}

	return newFile(d.project, d.bucketname, downloader), nil
}

func isDir(ctx context.Context, d *Dir, objectKey string) bool {
	objectIter := d.project.ListObjects(ctx,
		d.bucketname,
		&uplink.ListObjectsOptions{
			Prefix:    objectKey + "/",
			Recursive: true,
		},
	)
	if objectIter.Next() {
		item := objectIter.Item()
		if strings.Contains(item.Key, objectKey+"/") || strings.Contains(item.Key, "/"+objectKey+"/") {
			return true
		}
	}
	return false
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Println("dir.ReadDirAll called")
	start := time.Now()
	var dirEntries = []fuse.Dirent{}

	iter := d.project.ListObjects(ctx, d.bucketname, &uplink.ListObjectsOptions{Prefix: d.prefix})
	for iter.Next() {
		key := strings.TrimPrefix(iter.Item().Key, d.prefix)
		key = strings.TrimSuffix(key, "/")
		entry := fuse.Dirent{
			Name: key,
			Type: fuse.DT_File,
		}
		if iter.Item().IsPrefix {
			entry.Type = fuse.DT_Dir
		}
		dirEntries = append(dirEntries, entry)
	}
	if err := iter.Err(); err != nil {
		return dirEntries, logE("iter.Err", err)
	}

	log.Println(time.Since(start).Milliseconds(), "ms, dir ReadDirAll")
	log.Println(dirEntries)
	return dirEntries, nil
}

type File struct {
	project          *uplink.Project
	bucketname       string
	downloader       *uplink.Download
	downloaderOffset int64
}

var _ fs.Node = (*File)(nil)
var _ fs.HandleReader = (*File)(nil)
var _ fs.HandleReleaser = (*File)(nil)

func newFile(project *uplink.Project, bucketname string, downloader *uplink.Download) *File {
	return &File{
		project:    project,
		bucketname: bucketname,
		downloader: downloader,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Println("file.Attr called")
	a.Mode = 0o440
	a.Uid = uid
	a.Gid = gid
	a.Size = uint64(f.downloader.Info().System.ContentLength)
	a.Ctime = f.downloader.Info().System.Created
	a.Mtime = f.downloader.Info().System.Created
	a.Crtime = f.downloader.Info().System.Created
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Println("file.Read called", f.downloader.Info().Key)

	if req.Offset != f.downloaderOffset || f.downloader == nil {
		log.Println("req.Offset != f.downloaderOffset", req.Offset, f.downloaderOffset)
		if f.downloader != nil {
			err := f.downloader.Close()
			f.downloader = nil
			if err != nil {
				logE("downloader.Close()", err)
			}
		}
		downloader, err := f.project.DownloadObject(context.Background(),
			f.bucketname,
			f.downloader.Info().Key,
			&uplink.DownloadOptions{Offset: req.Offset, Length: int64(-1)},
		)
		if err != nil {
			return logE("DownloadObject", err)
		}
		f.downloader = downloader
		f.downloaderOffset = 0
	}

	buf := make([]byte, req.Size)
	n, err := f.downloader.Read(buf)
	if err != nil {
		if err != io.EOF {
			return logE("Read", err)
		}
	}
	resp.Data = buf[:n]
	f.downloaderOffset += int64(n)
	return nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if f.downloader == nil {
		return nil
	}
	return f.downloader.Close()
}
