package main

import (
	"context"
	"errors"
	"fmt"
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

func NewFS(project *uplink.Project, bucketname string) *FS {
	return &FS{
		root: NewDir(project, bucketname, ""),
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
	fmt.Println("dir.Attr called")
	a.Mode = os.ModeDir | 0o555
	a.Uid = uid
	a.Gid = gid
	return nil
}

func (d *Dir) Lookup(ctx context.Context, objectKey string) (fs.Node, error) {
	fmt.Println("dir.Lookup called")
	start := time.Now()

	objectKey = d.prefix + objectKey
	object, err := d.project.StatObject(ctx, d.bucketname, objectKey)
	if err != nil {
		if errors.Is(err, uplink.ErrObjectNotFound) {
			if ok := isDir(ctx, d, objectKey); ok {
				// todo: currently this will create a new dir if the object isn't found
				// ideally we want to listObjects to see if the objectKey is a dir
				// and return err syscall.ENOENT if its not a dir either
				// however we are deciding to do this hack for now for performance gain
				d := NewDir(d.project, d.bucketname, objectKey+"/")
				fmt.Println(time.Since(start).Milliseconds(),
					" ms, prefix dir lookup for object:", objectKey,
				)
				return d, nil
			}
		}
		return nil, logE("lookup object or dir does not exist", syscall.ENOENT)
	}

	downloader, err := d.project.DownloadObject(ctx,
		d.bucketname,
		object.Key,
		&uplink.DownloadOptions{Length: int64(-1)},
	)
	// defer ds.Close()
	if err != nil {
		return nil, logE("DownloadObject", err)
	}

	f := newFile(object, d.project, d.bucketname, downloader)
	fmt.Println(time.Since(start).Milliseconds(),
		" ms, file dir lookup for object", object,
	)
	return f, nil
}

func isDir(ctx context.Context, d *Dir, objectKey string) bool {
	fmt.Println("isDir")
	objectIter := d.project.ListObjects(ctx, d.bucketname, &uplink.ListObjectsOptions{Recursive: true})
	for objectIter.Next() {
		item := objectIter.Item()
		fmt.Println("objectKey:", objectKey)
		fmt.Println("item.Key:", item.Key)
		// key := strings.TrimPrefix(item.Key, d.prefix)
		// key = strings.TrimSuffix(key, "/")
		// fmt.Println("key:", key)
		// if item.key contains key+/ or /+key+/
		if strings.Contains(item.Key, objectKey+"/") || strings.Contains(item.Key, "/"+objectKey+"/") {
			return true
		}
		// if key == objectKey {
		// 	return true
		// }
	}
	return false
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fmt.Println("dir.ReadDirAll called")
	start := time.Now()
	var dirDirs = []fuse.Dirent{}

	iter := d.project.ListObjects(ctx, d.bucketname, &uplink.ListObjectsOptions{Prefix: d.prefix})
	for iter.Next() {
		key := strings.TrimPrefix(iter.Item().Key, d.prefix)
		key = strings.TrimSuffix(key, "/")
		fmt.Println("list:", iter.Item().Key, key)
		entry := fuse.Dirent{
			Name: key,
			Type: fuse.DT_File,
		}
		if iter.Item().IsPrefix {
			fmt.Println("is prfix")
			entry.Type = fuse.DT_Dir
		}
		dirDirs = append(dirDirs, entry)
	}
	if err := iter.Err(); err != nil {
		return dirDirs, logE("iter.Err", err)
	}

	fmt.Println(time.Since(start).Milliseconds(), "ms, dir ReadDirAll")
	fmt.Println(dirDirs)
	return dirDirs, nil
}

type File struct {
	obj              *uplink.Object
	project          *uplink.Project
	bucketname       string
	downloader       *uplink.Download
	downloaderOffset int64
}

var _ fs.Node = (*File)(nil)
var _ fs.HandleReader = (*File)(nil)

func newFile(obj *uplink.Object, project *uplink.Project, bucketname string, downloader *uplink.Download) *File {
	return &File{
		obj:        obj,
		project:    project,
		bucketname: bucketname,
		downloader: downloader,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Println("file.Attr called")
	start := time.Now()
	a.Mode = 0o444
	a.Uid = uid
	a.Gid = gid

	s, err := f.project.StatObject(ctx, f.bucketname, f.obj.Key)
	if err != nil {
		return logE("file.Attr statObject", err)
	}
	a.Size = uint64(s.System.ContentLength)
	fmt.Println(time.Since(start).Milliseconds(), "ms, file Attr for", f.obj.Key)
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Println("file.Read called", f.obj.Key)
	if req.Offset != f.downloaderOffset {
		return logE(fmt.Sprintf("offset doesn't match. expected %d, got %d",
			f.downloaderOffset,
			req.Offset,
		), nil)
	}
	buf := make([]byte, req.Size)
	_, err := f.downloader.Read(buf)
	if err != nil {
		if err != io.EOF {
			return logE("Read", err)
		}
	}
	resp.Data = buf[:]
	f.downloaderOffset += int64(len(resp.Data))
	return nil
}
