package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
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

func main() {
	access := flag.String("access", "", "access grant to a Storj project")
	bucketname := flag.String("bucket", "", "name of the Storj bucket to be mounted")
	mountpoint := flag.String("mountpoint", "", "location to mount the Storj bucket")
	flag.Parse()

	c, err := fuse.Mount(
		*mountpoint,
		fuse.FSName("storj"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	project := setupUplink(ctx, *access, *bucketname)
	defer project.Close()

	pwd, err := os.Getwd()
	if err != nil {
		log.Println("getwd err:", err)
	}
	path := filepath.Join(pwd, *mountpoint)
	log.Println(fmt.Sprintf("starting fuse... mounting bucket sj://%s at path %s", *bucketname, path))

	err = fs.Serve(c, NewFS(project, *bucketname))
	if err != nil {
		log.Fatal(err)
	}
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
	project    *uplink.Project
	bucketname string
	prefix     string
}

// fs.Node is the interface required of a file or directory.
var _ fs.Node = (*Dir)(nil)

var _ fs.HandleReadDirAller = (*Dir)(nil)

func NewDir(project *uplink.Project, bucketname, prefix string) *Dir {
	return &Dir{
		prefix:     prefix,
		bucketname: bucketname,
		project:    project,
	}
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	a.Uid = uid
	a.Gid = gid
	if d.prefix != "" {
		s, err := d.project.StatObject(ctx, d.bucketname, d.prefix)
		if err != nil {
			fmt.Printf("dir stat: %#v\n", err)
			fmt.Printf("dir stat obj: %#v\n", s)
		}
		if s.IsPrefix {
			fmt.Printf("is prefix stat: %#v\n", err)
		}
		a.Size = uint64(s.System.ContentLength)
	}
	return nil
}

func (d *Dir) Lookup(ctx context.Context, objKey string) (fs.Node, error) {
	start := time.Now()

	if objKey == "" {
		return d, nil
	}
	object, err := d.project.StatObject(ctx, d.bucketname, objKey)
	if err != nil {
		if strings.Contains(err.Error(), "object not found") {
			fmt.Printf("err: %#v\n", err)
			return nil, syscall.ENOENT
		}
		return nil, err
	}

	if object.IsPrefix {
		d := NewDir(d.project, d.bucketname, object.Key)
		log.Println(time.Since(start).Milliseconds(),
			" ms, prefix dir lookup for object:", object.Key,
		)
		return d, nil
	}

	f := newFile(object, d.project, d.bucketname)
	log.Println(time.Since(start).Milliseconds(),
		" ms, file dir lookup for object", object,
	)
	return f, nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	start := time.Now()
	// A Dirent represents a single directory entry.
	var dirDirs = []fuse.Dirent{}

	fmt.Println("ListObjects:", d.bucketname)
	iter := d.project.ListObjects(ctx, d.bucketname, nil)
	for iter.Next() {
		fmt.Println("list:", iter.Item().Key)
		entry := fuse.Dirent{
			Name: iter.Item().Key,
			Type: fuse.DT_File,
		}
		if iter.Item().IsPrefix {
			fmt.Println("is prfix")
			entry.Type = fuse.DT_Dir
		}
		dirDirs = append(dirDirs, entry)
	}
	if err := iter.Err(); err != nil {
		log.Fatal("listObj: ", err)
		return dirDirs, err
	}

	log.Println(time.Since(start).Milliseconds(), "ms, dir ReadDirAll")
	fmt.Println(dirDirs)
	return dirDirs, nil
}

type File struct {
	obj        *uplink.Object
	project    *uplink.Project
	bucketname string
}

// fs.Node is the interface required of a file or directory.
var _ fs.Node = (*File)(nil)

var _ fs.HandleReadAller = (*File)(nil)

func newFile(obj *uplink.Object, project *uplink.Project, bucketname string) *File {
	return &File{
		obj:        obj,
		project:    project,
		bucketname: bucketname,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	start := time.Now()
	a.Mode = 0o444
	a.Uid = uid
	a.Gid = gid

	s, err := f.project.StatObject(ctx, f.bucketname, f.obj.Key)
	if err != nil {
		log.Fatal("object stat: ", err)
	}
	a.Size = uint64(s.System.ContentLength)
	log.Println(time.Since(start).Milliseconds(), "ms, file Attr for", f.obj.Key)
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	start := time.Now()
	object, err := f.project.DownloadObject(ctx, f.bucketname, f.obj.Key, nil)
	if err != nil {
		log.Fatal("download: ", err)
	}
	defer object.Close()

	b, err := ioutil.ReadAll(object)
	if err != nil {
		log.Fatal("readAll: ", err)
	}
	log.Println(time.Since(start).Milliseconds(), "ms, file ReadAll for", f.obj.Key)
	return b, nil
}
